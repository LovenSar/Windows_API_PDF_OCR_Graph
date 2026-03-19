#!/usr/bin/env python3
"""
Embedding 语义兜底 — 用向量相似度为孤立节点找最近邻并建边

对图谱中所有孤立节点，用 entity name + description 编码向量，
在已连通节点中找 top-k 最近邻，超过相似度阈值则建 semantically_related 边。

用法:
  python embed_link_isolated.py                     # 预览
  python embed_link_isolated.py --apply             # 写入
  python embed_link_isolated.py --model bge-m3      # 指定模型
  python embed_link_isolated.py --threshold 0.80    # 调整阈值
"""

import json
import os
import glob
import argparse
import numpy as np
from collections import defaultdict
from datetime import datetime, timezone

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")

DEFAULT_MODEL = "all-MiniLM-L6-v2"
DEFAULT_THRESHOLD = 0.82
DEFAULT_TOP_K = 3


WEAK_EDGE_TYPES = {"belongs_to_domain", "belongs_to_header"}


def load_entities_and_edges(exclude_weak=True):
    """Load entities and edge set from json_output_v4."""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )
    entities = {}
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
        for ent in doc.get("entities", []):
            name = ent.get("name", "").strip()
            if not name:
                continue
            entities[name] = {
                "entity_type": str(ent.get("entity_type", "unknown")).strip().lower(),
                "description": str(ent.get("description", ""))[:300],
            }

    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    neighbors = defaultdict(set)
    if os.path.exists(edges_path):
        with open(edges_path, "r", encoding="utf-8") as f:
            edge_doc = json.load(f)
        for e in edge_doc.get("edges", []):
            if exclude_weak and e.get("type", "") in WEAK_EDGE_TYPES:
                continue
            s = e.get("source", "")
            t = e.get("target", "")
            if s in entities:
                neighbors[s].add(t)
            if t in entities:
                neighbors[t].add(s)

    isolated = [n for n in entities if len(neighbors[n]) == 0]
    connected = [n for n in entities if len(neighbors[n]) > 0]

    return entities, isolated, connected, neighbors


def build_text(name, info):
    """Build text representation for embedding."""
    et = info.get("entity_type", "")
    desc = info.get("description", "")
    parts = [name]
    if et and et != "unknown":
        parts.append(f"({et})")
    if desc:
        parts.append(desc[:200])
    return " ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Embedding 语义兜底链接")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Embedding 模型 (默认: {DEFAULT_MODEL})")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help=f"相似度阈值 (默认: {DEFAULT_THRESHOLD})")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K, help=f"每个孤立节点最多连接 k 个 (默认: {DEFAULT_TOP_K})")
    parser.add_argument("--max-isolated", type=int, default=0, help="最多处理 N 个孤立节点 (0=全部)")
    parser.add_argument("--exclude-weak", action="store_true", default=True,
                        help="排除 domain/header 等弱连接边计算孤立性 (默认: True)")
    args = parser.parse_args()

    print("加载图谱数据...")
    entities, isolated, connected, neighbors = load_entities_and_edges(
        exclude_weak=args.exclude_weak
    )
    print(f"  总实体: {len(entities)}")
    print(f"  孤立节点: {len(isolated)} ({len(isolated)*100//len(entities)}%)")
    print(f"  连通节点: {len(connected)}")

    if not isolated:
        print("没有孤立节点，无需处理。")
        return

    if args.max_isolated > 0:
        isolated = isolated[:args.max_isolated]
        print(f"  限制处理: {len(isolated)} 个孤立节点")

    print(f"\n加载 embedding 模型: {args.model} ...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(args.model)

    isolated_texts = [build_text(n, entities[n]) for n in isolated]
    connected_texts = [build_text(n, entities[n]) for n in connected]

    print(f"编码 {len(isolated)} 个孤立节点...")
    iso_emb = model.encode(isolated_texts, show_progress_bar=True, batch_size=256)

    print(f"编码 {len(connected)} 个连通节点...")
    conn_emb = model.encode(connected_texts, show_progress_bar=True, batch_size=256)

    iso_emb = iso_emb / np.linalg.norm(iso_emb, axis=1, keepdims=True)
    conn_emb = conn_emb / np.linalg.norm(conn_emb, axis=1, keepdims=True)

    print("计算相似度矩阵...")
    sim_matrix = iso_emb @ conn_emb.T

    new_edges = []
    linked_count = 0

    for i, iso_name in enumerate(isolated):
        sims = sim_matrix[i]
        top_indices = np.argsort(sims)[::-1][:args.top_k]
        for idx in top_indices:
            score = float(sims[idx])
            if score < args.threshold:
                break
            target_name = connected[idx]
            new_edges.append({
                "source": iso_name,
                "target": target_name,
                "type": "semantically_related",
                "similarity": round(score, 4),
                "_enriched_by": "embedding_similarity",
            })
        if any(float(sims[idx]) >= args.threshold for idx in top_indices):
            linked_count += 1

    print(f"\n{'='*70}")
    print(f"结果汇总")
    print(f"{'='*70}")
    print(f"  阈值: {args.threshold}")
    print(f"  新增边: {len(new_edges)}")
    print(f"  被链接的孤立节点: {linked_count}/{len(isolated)}")
    new_iso = len(isolated) - linked_count
    total = len(entities)
    remaining_iso = (len(isolated) - linked_count) + (total - len(isolated) - len(connected))
    print(f"  预计新孤立率: {(len(isolated) - linked_count) * 100 / total:.1f}%")

    if new_edges:
        print(f"\n前 10 条示例边:")
        for e in new_edges[:10]:
            print(f"  {e['source']} --[{e['similarity']}]--> {e['target']}")

    if not args.apply:
        print("\n[预览模式] 使用 --apply 写入")
        return

    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    with open(edges_path, "r", encoding="utf-8") as f:
        edge_doc = json.load(f)
    existing = edge_doc.get("edges", [])

    existing_keys = set()
    for e in existing:
        existing_keys.add((e.get("source", ""), e.get("target", ""), e.get("type", "")))

    added = 0
    for e in new_edges:
        k = (e["source"], e["target"], e["type"])
        if k not in existing_keys:
            existing.append(e)
            existing_keys.add(k)
            added += 1

    edge_doc["edges"] = existing
    edge_doc["total_edges"] = len(existing)
    edge_doc["_schema"] = "api_edges_v4.5_embedding"
    with open(edges_path, "w", encoding="utf-8") as f:
        json.dump(edge_doc, f, ensure_ascii=False, indent=2)
    print(f"\n已更新 global_edges.json: 新增 {added} 条边, 总计 {len(existing)} 条")


if __name__ == "__main__":
    main()
