#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_entity_key_casing.py — §3.1 HIGH 离线修复脚本

历史问题:
  global_entity_index.json 含大小写冲突键 (PowerShell ConvertFrom-Json 拒收)。
  例: 'EvtSerCx2SetWaitMask' vs 'EvtSerCx2SetWaitmask'

本脚本:
  - 读 json_output_v4/global_entity_index.json
  - 把 entity keys 做大小写无关归一 (NFKC + 去 BOM/零宽 + 空白压缩)
  - 用 merge_entity_keys_case_insensitive() 合并冲突 (按字段数 + 文本长度评分)
  - 把合并结果写回
  - 默认 dry-run,只打印合并统计;--apply 才真正写盘

注意: 同时修复 global_edges.json 里 source/target 的 key casing 漂移 (因为 entity
index 改了,edges 里的端点也要跟着改)。

用法:
  python scripts/fix_entity_key_casing.py                        # dry-run
  python scripts/fix_entity_key_casing.py --apply                # 改写
  python scripts/fix_entity_key_casing.py --input <other.json>   # 其他输入
  python scripts/fix_entity_key_casing.py --backup               # 自动备份
  python scripts/fix_entity_key_casing.py --also-edges           # 同时修 edges

Author: Plan §5.2 实施
"""
import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pipeline_lib.config import (  # noqa: E402
    normalize_entity_key,
    merge_entity_keys_case_insensitive,
)


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def detect_case_collisions(entities: dict) -> dict:
    """返回 lowercase_key -> [orig_keys]"""
    groups = {}
    for k in entities:
        nk = normalize_entity_key(k)
        if not nk:
            continue
        lk = nk.lower()
        groups.setdefault(lk, []).append(k)
    return {lk: ks for lk, ks in groups.items() if len(ks) > 1}


def remap_keys_after_merge(original_keys: list, merge_log: dict) -> dict:
    """
    给定合并日志 {old_key: new_key}, 返回 {old_key: new_key}。
    用于重写 edges 里的 source/target 引用。
    """
    return {ok: nk for ok, nk in merge_log.items() if ok != nk}


def fix_edges_keys(edges: list, key_remap: dict) -> tuple:
    """按 key_remap 重写 edges 里的 source/target。返回 (new_edges, dangling)。"""
    new_edges = []
    dangling = 0
    for e in edges:
        ne = dict(e)
        src = ne.get("source")
        tgt = ne.get("target")
        if src in key_remap:
            ne["source"] = key_remap[src]
        if tgt in key_remap:
            ne["target"] = key_remap[tgt]
        new_edges.append(ne)
    return new_edges, dangling


def main():
    ap = argparse.ArgumentParser(description="§3.1 离线修复 entity key 大小写冲突")
    ap.add_argument("--input", default=None,
                    help="entity index 路径 (默认 json_output_v4/global_entity_index.json)")
    ap.add_argument("--output", default=None,
                    help="entity index 输出路径 (默认覆盖 input)")
    ap.add_argument("--also-edges", action="store_true",
                    help="同时修复 global_edges.json 的 source/target 引用")
    ap.add_argument("--edges-input", default=None,
                    help="edges 输入路径 (默认 json_output_v4/global_edges.json)")
    ap.add_argument("--edges-output", default=None,
                    help="edges 输出路径 (默认覆盖 edges-input)")
    ap.add_argument("--apply", action="store_true", help="实际写盘")
    ap.add_argument("--backup", action="store_true", help="改写前自动备份")
    ap.add_argument("--report", default=None,
                    help="merge 报告输出路径 (JSON, 默认 _key_casing_report_<时间戳>.json)")
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_input = os.path.join(root, "json_output_v4", "global_entity_index.json")
    default_edges = os.path.join(root, "json_output_v4", "global_edges.json")

    input_path = args.input or default_input
    output_path = args.output or input_path
    edges_input = args.edges_input or default_edges
    edges_output = args.edges_output or edges_input

    if not os.path.isfile(input_path):
        print(f"ERROR: 输入文件不存在: {input_path}", file=sys.stderr)
        return 2

    data = load_json(input_path)
    # 容错: dict 顶层是 {entities: {...}} 还是直接是 entities dict
    if isinstance(data, dict) and "entities" in data and isinstance(data["entities"], dict):
        entities = data["entities"]
        has_wrapper = True
    else:
        entities = data
        has_wrapper = False

    print(f"输入: {input_path}")
    print(f"entity 数: {len(entities)}")

    collisions = detect_case_collisions(entities)
    print(f"\n大小写冲突组数: {len(collisions)}")
    if collisions:
        print("\n=== 冲突样本 (前 10 组) ===")
        for lk, ks in list(collisions.items())[:10]:
            print(f"  '{lk}' -> {ks}")

    merged = merge_entity_keys_case_insensitive(entities)
    n_before = len(entities)
    n_after = len(merged)
    print(f"\n合并后 entity 数: {n_before} -> {n_after} (减少 {n_before - n_after})")

    # 构造 old_key -> new_key 重映射,用于修 edges
    # 注意: 一个 old_key 在合并后变成某个 winner,所以反向查表
    key_remap = {}
    for lk, ks in collisions.items():
        # 找出 winner (合并结果里 lowercase 匹配的那一个)
        new_key = None
        for nk in merged:
            if nk.lower() == lk:
                new_key = nk
                break
        if new_key:
            for ok in ks:
                key_remap[ok] = new_key
    print(f"\nedges 重映射条目数: {len(key_remap)}")

    report = {
        "input": input_path,
        "n_entities_before": n_before,
        "n_entities_after": n_after,
        "collision_groups": {lk: ks for lk, ks in collisions.items()},
        "key_remap": key_remap,
    }
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = args.report or os.path.join(
            os.path.dirname(input_path), f"_key_casing_report_{ts}.json"
        )

    if not args.apply:
        print("\n[dry-run] 加 --apply 才实际写盘")
        # 即便 dry-run 也写 report(到工作目录或固定名),供用户审阅
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = args.report or os.path.join(
            os.path.dirname(input_path), f"_key_casing_report_{ts}.json"
        )
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"报告: {report_path}")
        return 0

    # apply 模式
    if args.backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = input_path + f".bak.{ts}"
        import shutil
        shutil.copy2(input_path, backup_path)
        print(f"已备份 entity: {backup_path}")

    # 写回 entity index
    if has_wrapper:
        data["entities"] = merged
        save_json(output_path, data)
    else:
        save_json(output_path, merged)
    print(f"[apply] 已写入 entity: {output_path}")

    # 报告也写
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = args.report or os.path.join(
        os.path.dirname(input_path), f"_key_casing_report_{ts}.json"
    )
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"报告: {report_path}")

    # 可选: 同时修 edges
    if args.also_edges:
        if os.path.isfile(edges_input):
            with open(edges_input, "r", encoding="utf-8") as f:
                edges_data = json.load(f)
            if isinstance(edges_data, dict) and "edges" in edges_data:
                edges = edges_data["edges"]
                edges_wrapped = True
            elif isinstance(edges_data, list):
                edges = edges_data
                edges_wrapped = False
            else:
                print(f"WARN: edges 文件格式未知,跳过: {edges_input}")
                return 0

            new_edges, _ = fix_edges_keys(edges, key_remap)
            if edges_wrapped:
                edges_data["edges"] = new_edges
                save_obj = edges_data
            else:
                save_obj = new_edges

            if args.backup:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = edges_input + f".bak.{ts}"
                import shutil
                shutil.copy2(edges_input, backup_path)
                print(f"已备份 edges: {backup_path}")

            save_json(edges_output, save_obj)
            print(f"[apply] 已写入 edges: {edges_output}")
        else:
            print(f"WARN: edges 文件不存在,跳过: {edges_input}")

    return 0


if __name__ == "__main__":
    sys.exit(main())