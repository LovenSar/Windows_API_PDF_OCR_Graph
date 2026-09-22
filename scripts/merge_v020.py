#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_v020.py — 合并 desktop-src (Win32 API) + driver-docs-ddi (Driver DDI) 抽取产物

输入: 两个 v0.2.0 抽取 JSON (含 entities + edges)
输出: 合并后的单一 JSON (entities + edges 去重)
"""
import argparse
import json
import os
import sys
from collections import defaultdict, Counter


def merge(win32_json: str, ddi_json: str, output: str):
    with open(win32_json, encoding="utf-8") as f:
        win32 = json.load(f)
    with open(ddi_json, encoding="utf-8") as f:
        ddi = json.load(f)

    # 合并 entities — 冲突时优先保留字段数多的
    ents = {}
    for src in [win32["entities"], ddi["entities"]]:
        for k, v in src.items():
            if k not in ents:
                ents[k] = v
            else:
                # 取字段更多的 (更详细)
                if len(v) > len(ents[k]):
                    ents[k] = v

    # 合并 edges — 去重 (src, tgt, type)
    edge_set = set()
    edges = []
    for src in [win32["edges"], ddi["edges"]]:
        for e in src:
            k = (e["source"], e["target"], e["type"])
            if k not in edge_set:
                edge_set.add(k)
                edges.append(e)

    # 统计
    print(f"=== merge_v020 ===")
    print(f"  desktop-src entities: {len(win32['entities'])}")
    print(f"  DDI entities:         {len(ddi['entities'])}")
    print(f"  merged entities:      {len(ents)}")
    print(f"  desktop-src edges:    {len(win32['edges'])}")
    print(f"  DDI edges:            {len(ddi['edges'])}")
    print(f"  merged edges:         {len(edges)}")

    # 输出
    out = {
        "_schema": "v0.2.0_markdown_full",
        "_source": "MicrosoftDocs/win32 (desktop-src) + MicrosoftDocs/windows-driver-docs-ddi",
        "_generated_at": __import__("datetime").datetime.now().isoformat(),
        "entities": ents,
        "edges": edges,
    }
    with open(output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n已写入: {output}")

    # 摘要
    et = Counter(v.get("entity_type", "?") for v in ents.values())
    print(f"\n  entity_type 分布: {dict(et)}")
    eet = Counter(e["type"] for e in edges)
    print(f"  edge_type 分布: {dict(eet)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--win32", required=True)
    ap.add_argument("--ddi", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    merge(args.win32, args.ddi, args.output)