#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_edge_types.py — §3.2 HIGH 离线修复脚本

历史问题:
  global_edges.json 出现 180+ distinct edge types (LLM refine 自由发挥产生)。
  设计目标只有 ~10 个 strong/structural/header/domain + 1 个 related 兜底。

本脚本:
  - 读 json_output_v4/global_edges.json (或 backup 路径)
  - 用 pipeline_lib.config.normalize_edge_type() 把每条边的 type 字段归一
  - 用 merge 计数器: 同一 (source, target) + 归一后 type 的边合并计数
  - 输出去重后的边列表
  - 默认 dry-run,只打印 before/after 统计;--apply 才真正写盘

用法:
  python scripts/fix_edge_types.py                        # dry-run 真实数据
  python scripts/fix_edge_types.py --apply               # 改写真实数据
  python scripts/fix_edge_types.py --input <other.json>  # 指定其他输入文件
  python scripts/fix_edge_types.py --backup              # 自动备份 .bak.<时间戳>

Author: Plan §5.2 实施
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime

# 让脚本能从 scripts/ 目录里 import pipeline_lib
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pipeline_lib.config import normalize_edge_type  # noqa: E402


def load_edges(path: str) -> list:
    """读取 JSON,容错处理大小写冲突键(虽然 global_edges.json 实际没有此问题)"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "edges" in data:
        return data["edges"]
    if isinstance(data, list):
        return data
    raise ValueError(f"无法解析 {path}: 期望 list 或 dict.edges")


def save_edges(path: str, edges: list) -> None:
    """写回 JSON,保留外层 dict 结构(若原文件是 dict)"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "edges" in data:
        data["edges"] = edges
        out = data
    elif isinstance(data, list):
        out = edges
    else:
        out = edges
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def normalize_edges(edges: list) -> tuple:
    """
    返回 (new_edges, type_counter_before, type_counter_after, merged_count)
    """
    type_before = Counter()
    type_after = Counter()
    merged = 0

    # 先按 (source, target, normalized_type) 聚合; 同 group 内合并 weight / attributes
    bucket = {}
    for e in edges:
        src = e.get("source")
        tgt = e.get("target")
        typ = e.get("type", "")
        type_before[typ] += 1
        norm = normalize_edge_type(typ)
        type_after[norm] += 1
        key = (src, tgt, norm)
        if key in bucket:
            # 已存在: 合并(weight 累加, attributes 取首个非空)
            existing = bucket[key]
            # 计数合并
            existing["_count"] = existing.get("_count", 1) + 1
            merged += 1
            # weight 累加
            w_new = e.get("weight", 1)
            w_old = existing.get("weight", 1)
            existing["weight"] = (w_old or 1) + (w_new or 1) - 1  # 不重复计首条
            # attributes 取长补短
            for ak, av in (e.get("attributes") or {}).items():
                if ak not in existing.get("attributes", {}) or not existing["attributes"].get(ak):
                    existing.setdefault("attributes", {})[ak] = av
        else:
            new_e = dict(e)
            new_e["type"] = norm
            new_e["_count"] = 1
            new_e.setdefault("weight", 1)
            bucket[key] = new_e

    return list(bucket.values()), type_before, type_after, merged


def main():
    ap = argparse.ArgumentParser(description="§3.2 离线修复边类型")
    ap.add_argument("--input", default=None,
                    help="输入 JSON 路径 (默认 json_output_v4/global_edges.json)")
    ap.add_argument("--output", default=None,
                    help="输出 JSON 路径 (默认覆盖 input)")
    ap.add_argument("--apply", action="store_true",
                    help="实际写盘,否则只 dry-run 打印 diff")
    ap.add_argument("--backup", action="store_true",
                    help="改写前先备份原文件为 .bak.<时间戳>")
    ap.add_argument("--top", type=int, default=30,
                    help="dry-run 时打印的 top-N type 数 (默认 30)")
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_input = os.path.join(root, "json_output_v4", "global_edges.json")
    input_path = args.input or default_input
    output_path = args.output or input_path

    if not os.path.isfile(input_path):
        print(f"ERROR: 输入文件不存在: {input_path}", file=sys.stderr)
        return 2

    edges = load_edges(input_path)
    print(f"输入: {input_path}")
    print(f"边数: {len(edges)}")

    new_edges, type_before, type_after, merged = normalize_edges(edges)

    print(f"\n=== BEFORE (top {args.top}) ===")
    for t, c in type_before.most_common(args.top):
        print(f"  {c:6d}  {t!r}")
    print(f"  (总共 {len(type_before)} 个 distinct type)")
    print(f"\n=== AFTER (top {args.top}) ===")
    for t, c in sorted(type_after.items(), key=lambda x: -x[1]):
        print(f"  {c:6d}  {t!r}")
    print(f"  (总共 {len(type_after)} 个 distinct type, 从 {len(type_before)} 收敛)")

    print(f"\n合并: {merged} 条同 (src,tgt,type) 边被聚合计数")
    print(f"边数: {len(edges)} -> {len(new_edges)}")

    if not args.apply:
        print("\n[dry-run] 加 --apply 才实际写盘")
        return 0

    if args.backup and input_path == output_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = input_path + f".bak.{ts}"
        import shutil
        shutil.copy2(input_path, backup_path)
        print(f"已备份: {backup_path}")

    save_edges(output_path, new_edges)
    print(f"\n[apply] 已写入: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())