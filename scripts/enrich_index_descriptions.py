#!/usr/bin/env python3
"""
将各文档 JSON 中的 description 回填到 global_entity_index.json

原因：历史版本 pipeline 写入全局索引时只含 id/file/type，graph_viewer 读不到描述。
本脚本扫描 json_output_v4 下实体文件，按实体名合并最长 description 后写回索引。

用法:
  python scripts/enrich_index_descriptions.py           # 预览
  python scripts/enrich_index_descriptions.py --apply   # 写入
"""

from __future__ import annotations

import argparse
import glob
import json
import os
from collections import OrderedDict
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT = os.path.join(ROOT, "json_output_v4")
MAX_DESC = 8000


def _trunc(s: str) -> str:
    s = (s or "").replace("\n", " ").strip()
    if not s:
        return ""
    return s if len(s) <= MAX_DESC else s[:MAX_DESC] + "..."


def collect_descriptions(out_dir: str) -> dict[str, str]:
    """name -> longest description seen"""
    best: dict[str, str] = {}
    paths = sorted(
        p
        for p in glob.glob(os.path.join(out_dir, "*.json"))
        if not os.path.basename(p).startswith("_")
        and not os.path.basename(p).startswith("global")
    )
    for fp in paths:
        try:
            with open(fp, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            continue
        for ent in doc.get("entities", []):
            name = (ent.get("name") or "").strip()
            if not name:
                continue
            raw = ent.get("description") or ""
            d = _trunc(raw)
            if not d:
                continue
            if len(d) > len(best.get(name, "")):
                best[name] = d
    return best


def main() -> None:
    ap = argparse.ArgumentParser(description="回填 global_entity_index 的 description")
    ap.add_argument("--dir", default=DEFAULT_OUT, help="json_output_v4 目录")
    ap.add_argument("--apply", action="store_true", help="写回 global_entity_index.json")
    args = ap.parse_args()

    idx_path = os.path.join(args.dir, "global_entity_index.json")
    if not os.path.isfile(idx_path):
        raise SystemExit(f"未找到: {idx_path}")

    with open(idx_path, encoding="utf-8") as f:
        idx_doc = json.load(f)
    entities = idx_doc.get("entities") or {}
    if not isinstance(entities, dict):
        raise SystemExit("global_entity_index.json 格式异常: entities 应为对象")

    desc_map = collect_descriptions(args.dir)
    filled = 0
    upgraded = 0
    sample: list[tuple[str, int]] = []

    new_entities = OrderedDict()
    for name in sorted(entities.keys()):
        row = entities[name]
        if not isinstance(row, dict):
            new_entities[name] = row
            continue
        merged = dict(row)
        d_new = desc_map.get(name, "")
        d_old = (merged.get("description") or "").strip()
        if d_new:
            if not d_old:
                filled += 1
                if len(sample) < 5:
                    sample.append((name, len(d_new)))
            elif len(d_new) > len(d_old):
                upgraded += 1
            merged["description"] = d_new
        new_entities[name] = merged

    print(f"扫描到带描述的实体名: {len(desc_map):,}")
    print(f"索引实体数: {len(new_entities):,}")
    print(f"新增描述字段: {filled:,} | 用更长描述覆盖: {upgraded:,}")
    if sample:
        print("示例（新增）:")
        for n, ln in sample:
            print(f"  {n[:56]:<56}  len={ln}")

    if not args.apply:
        print("\n[预览] 加 --apply 写入 global_entity_index.json")
        return

    idx_doc["entities"] = new_entities
    idx_doc["_descriptions_enriched_at"] = datetime.now(timezone.utc).isoformat()
    tmp = idx_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(idx_doc, f, ensure_ascii=False, indent=2)
    os.replace(tmp, idx_path)
    print(f"\n已写入: {idx_path}")


if __name__ == "__main__":
    main()
