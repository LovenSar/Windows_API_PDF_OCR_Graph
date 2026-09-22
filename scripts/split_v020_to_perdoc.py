#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
split_v020_to_perdoc.py — 把 v0.1.2 global JSON 拆为 per-topic JSON (与 v0.1.0 70 doc 结构对齐)

输入:
  - global_entity_index.json (含 source_file 字段)
  - global_edges.json (src/tgt entity id 列表)

输出:
  - 每个 topic (parent dir) 对应一份 _md_<topic>_<timestamp>.json
  - 文件内含该 topic 所有 markdown 文档的 entities + 该 topic entities 出/入的 edges

用法:
  python scripts/split_v020_to_perdoc.py
      --ent  E:\WorkSpace\Windows_API_PDF_OCR_Graph\json_output_v4_v020_markdown\global_entity_index.json
      --edge E:\WorkSpace\Windows_API_PDF_OCR_Graph\json_output_v4_v020_markdown\global_edges.json
      --out  E:\WorkSpace\Windows_API_PDF_OCR_Graph\json_output_v4_v020_markdown
      --timestamp 20260921_2200

Author: v0.1.2 实施
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def topic_from_source_file(src_path: str) -> str:
    """从 source_file 路径提取 topic key。
    路径示例:
      - desktop-src/wic/nf-wic-...md → 'wic'
      - wdk-ddi-src/content/ntifs/nf-...md → 'ntifs'
      - wdk-ddi-src/content/_bltooth/index.md → '_bltooth'
    """
    if not src_path:
        return "unknown"
    s = src_path.replace("\\", "/")
    parts = s.split("/")
    # 倒数第二段通常是 topic dir
    return parts[-2] if len(parts) >= 2 else "unknown"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ent", required=True)
    ap.add_argument("--edge", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--timestamp", default="20260921_2200")
    args = ap.parse_args()

    with open(args.ent, encoding="utf-8") as f:
        d_ent = json.load(f)
    with open(args.edge, encoding="utf-8") as f:
        d_edge = json.load(f)

    entities = d_ent.get("entities", d_ent)
    edges = d_edge.get("edges", d_edge)
    print(f"输入: {len(entities)} entities, {len(edges)} edges")

    # 1. 按 topic 分组 entities
    by_topic = defaultdict(dict)  # topic -> {entity_id: data}
    for k, v in entities.items():
        topic = topic_from_source_file(v.get("source_file", ""))
        by_topic[topic][k] = v

    print(f"topics: {len(by_topic)}")

    # 2. 构建 topic -> entity_id 集合 (用于分配 edges)
    topic_to_eids = {t: set(ents.keys()) for t, ents in by_topic.items()}

    # 3. 按 topic 分组 edges (src 或 tgt 在该 topic entity 集合里就归入)
    edges_by_topic = defaultdict(list)
    for e in edges:
        s, t = e.get("source"), e.get("target")
        matched_topics = []
        for topic, eids in topic_to_eids.items():
            if s in eids or t in eids:
                matched_topics.append(topic)
        if matched_topics:
            for t in matched_topics:
                edges_by_topic[t].append(e)
        else:
            edges_by_topic["__unmatched__"].append(e)

    # 4. 写 per-topic 文件
    os.makedirs(args.out, exist_ok=True)
    written = 0
    total_ent = 0
    total_edge = 0
    for topic, ents in by_topic.items():
        fname = f"_md_{topic}_{args.timestamp}.json"
        out_path = os.path.join(args.out, fname)
        doc_edges = edges_by_topic.get(topic, [])
        doc_obj = {
            "_schema": "per_topic_v0.1.2_markdown",
            "_topic": topic,
            "_n_source_files": len(set(e.get("source_file", "") for e in ents.values())),
            "_extracted_at": datetime.now().isoformat(),
            "entities": ents,
            "edges": doc_edges,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc_obj, f, ensure_ascii=False, indent=2)
        written += 1
        total_ent += len(ents)
        total_edge += len(doc_edges)

    # 5. 写未匹配的 edges
    if edges_by_topic.get("__unmatched__"):
        out_path = os.path.join(args.out, f"_unmatched_edges_{args.timestamp}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"edges": edges_by_topic["__unmatched__"]}, f, ensure_ascii=False, indent=2)
        print(f"未匹配 edges: {len(edges_by_topic['__unmatched__'])}")

    print(f"\n=== 拆分完成 ===")
    print(f"  per-topic 文件数: {written}")
    print(f"  entities 分配: {total_ent} (输入 {len(entities)})")
    print(f"  edges 分配:    {total_edge} (输入 {len(edges)})")
    print(f"\n输出目录: {args.out}")


if __name__ == "__main__":
    sys.exit(main())