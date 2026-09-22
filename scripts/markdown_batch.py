#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
markdown_batch.py — 批量从 MicrosoftDocs/win32 desktop-src 抽取所有可用目录

输入: desktop-src 根目录 + OCR_raw 中的 topic 列表
输出: 合并的 JSON (entities + edges),与现有 pipeline.py 兼容
"""
import argparse
import json
import os
import sys
import time
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts.markdown_to_entities import process_directory  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desktop-src", required=True,
                    help="MicrosoftDocs/win32 desktop-src 路径")
    ap.add_argument("--ocr-root", required=True,
                    help="OCR_raw 根目录,用于提取 topic 列表")
    ap.add_argument("--output", required=True, help="输出 JSON")
    ap.add_argument("--topic-prefix", default="windows::",
                    help="实体 ID 前缀")
    args = ap.parse_args()

    # 1. 从 OCR_raw 提取 topic 列表
    ocr_topics = set()
    for fn in os.listdir(args.ocr_root):
        # 文件名格式: [OCR]_windows-<domain>-_<topic>_<date>.txt
        # 提取 _<topic>_ 段
        import re
        m = re.search(r"_(?P<topic>[a-z][a-z0-9_]+)_\d{8}_", fn)
        if m:
            ocr_topics.add(m.group("topic"))
    print(f"OCR topics: {len(ocr_topics)}")

    # 2. 找出 desktop-src 下可用的 topic
    available = []
    missing = []
    for t in sorted(ocr_topics):
        path = os.path.join(args.desktop_src, t)
        if os.path.isdir(path):
            available.append(t)
        else:
            missing.append(t)

    print(f"Available in desktop-src: {len(available)}")
    print(f"Missing: {len(missing)}")
    if missing:
        print(f"  Missing list: {missing[:20]}{'...' if len(missing) > 20 else ''}")

    # 3. 处理每个可用目录 (或整个 desktop-src)
    all_entities = {}
    all_edges = []
    per_topic_stats = []
    t0 = time.time()

    # 如果指定 --all-md,递归处理 desktop-src 下所有 .md 文件
    if os.environ.get("MARKDOWN_BATCH_ALL_MD"):
        print(f"递归处理 {args.desktop_src} 下所有 .md (MARKDOWN_BATCH_ALL_MD)")
        ents, edges = process_directory(args.desktop_src, "ALL")
        for k, v in ents.items():
            v["api_topic"] = "all"
            all_entities[k] = v
        all_edges.extend(edges)
        per_topic_stats.append(("all", len(ents), len(edges)))
    else:
        for t in available:
            path = os.path.join(args.desktop_src, t)
            ents, edges = process_directory(path, t)
            for k, v in ents.items():
                v["api_topic"] = t
                if k not in all_entities:
                    all_entities[k] = v
                else:
                    # 冲突时取更详细版本 (字段数更多)
                    if len(v) > len(all_entities[k]):
                        all_entities[k] = v
            all_edges.extend(edges)
            per_topic_stats.append((t, len(ents), len(edges)))

    elapsed = time.time() - t0
    print(f"\n=== 处理完成 ===")
    print(f"  时间: {elapsed:.1f} 秒")
    print(f"  实体 (合并后): {len(all_entities)}")
    print(f"  边 (累加):     {len(all_edges)}")

    # 4. 边去重 (src, tgt, type 三元组)
    seen = set()
    dedup_edges = []
    for e in all_edges:
        k = (e["source"], e["target"], e["type"])
        if k not in seen:
            seen.add(k)
            dedup_edges.append(e)
    print(f"  边 (去重后):   {len(dedup_edges)} (移除 {len(all_edges) - len(dedup_edges)} 重复)")

    # 5. 输出
    out = {
        "_schema": "extracted_v0.2.0_markdown",
        "_source": "MicrosoftDocs/win32 (Markdown)",
        "_generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "_topics_processed": len(available),
        "_topics_missing": missing,
        "entities": all_entities,
        "edges": dedup_edges,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n已写入: {args.output}")

    # 6. 报告每个 topic 的产出
    print(f"\n=== 各 topic 产出 (前 15) ===")
    for t, n, e in sorted(per_topic_stats, key=lambda x: -x[1])[:15]:
        print(f"  {t:30s}  ents={n:5d}  edges={e:5d}")
    if len(per_topic_stats) > 15:
        print(f"  ... and {len(per_topic_stats) - 15} more")

    # 7. 类型分布
    print(f"\n=== 实体类型分布 ===")
    for t, c in Counter(v.get("entity_type", "?") for v in all_entities.values()).most_common():
        print(f"  {t:25s} {c:6d}")
    print(f"\n=== 边类型分布 ===")
    for t, c in Counter(e["type"] for e in dedup_edges).most_common():
        print(f"  {t:25s} {c:6d}")


if __name__ == "__main__":
    sys.exit(main())