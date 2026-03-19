#!/usr/bin/env python3
"""
一次性 entity_type 脏数据清洗脚本

扫描 json_output_v4/ 下所有实体 JSON，将脏 entity_type 归一化后写回。
同时重新生成 global_entity_index.json 和 _refinement_report.json 中的类型统计。

用法:
  python fix_entity_types.py           # 预览模式（只显示，不写入）
  python fix_entity_types.py --apply   # 实际修改文件
"""

import json
import os
import glob
import argparse
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(THIS_DIR)
OUT_DIR = os.path.join(ROOT_DIR, "json_output_v4")
MAX_DESC_IN_GLOBAL_INDEX = 8000

ALLOWED_ENTITY_TYPES = {
    "function", "structure", "enum", "callback", "macro",
    "constant", "typedef", "union", "interface", "ioctl", "event",
    "method", "property", "notification", "oid", "enum_value",
    "error_code", "parameter", "application", "enum_member",
    "function_pointer", "flags", "structure_member", "field", "message",
    "technology", "attribute", "class", "unknown",
}

_TYPE_SYNONYMS = {
    "struct":      "structure",
    "structur":    "structure",
    "structures":  "structure",
    "flag":        "flags",
    "enumvalue":   "enum_value",
}


def _truncate_desc(text):
    if not text:
        return ""
    text = str(text).replace("\n", " ").strip()
    return text if len(text) <= MAX_DESC_IN_GLOBAL_INDEX else text[: MAX_DESC_IN_GLOBAL_INDEX] + "..."


def normalize_entity_type(et):
    if not et:
        return "unknown"
    et = str(et).strip().lower()
    if len(et) > 40:
        return "unknown"
    syn = _TYPE_SYNONYMS.get(et)
    if syn:
        return syn
    if et not in ALLOWED_ENTITY_TYPES:
        return "unknown"
    return et


def main():
    parser = argparse.ArgumentParser(description="entity_type 脏数据清洗")
    parser.add_argument("--apply", action="store_true", help="实际修改文件")
    args = parser.parse_args()

    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    print(f"扫描 {len(files)} 个实体 JSON 文件...\n")

    fix_log = defaultdict(list)
    total_entities = 0
    total_fixed = 0
    type_before = defaultdict(int)
    type_after = defaultdict(int)
    files_modified = 0

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except Exception as e:
            print(f"  跳过 {os.path.basename(fp)}: {e}")
            continue

        modified = False
        for ent in doc.get("entities", []):
            total_entities += 1
            raw = ent.get("entity_type", "")
            norm = normalize_entity_type(raw)
            type_before[str(raw).strip().lower() if raw else "(empty)"] += 1
            type_after[norm] += 1

            if str(raw).strip().lower() != norm:
                ent_name = ent.get("name", "?")
                fix_log[f"{raw!r} -> {norm}"].append(ent_name)
                if args.apply:
                    if raw is not None and raw != norm:
                        ent["_type_raw"] = raw
                    ent["entity_type"] = norm
                modified = True
                total_fixed += 1

        if modified and args.apply:
            tmp = fp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            os.replace(tmp, fp)
            files_modified += 1

    print("=" * 70)
    print("entity_type 修复汇总")
    print("=" * 70)
    print(f"  总实体数:     {total_entities}")
    print(f"  需修复:       {total_fixed}")
    print(f"  修改文件数:   {files_modified if args.apply else '(预览模式)'}")
    print()

    if fix_log:
        print("修复详情:")
        for change, names in sorted(fix_log.items(), key=lambda x: -len(x[1])):
            preview = ", ".join(names[:5])
            if len(names) > 5:
                preview += f" ...共 {len(names)} 个"
            print(f"  {change}  ({len(names)} 个实体)")
            print(f"    示例: {preview}")
        print()

    print("修复后 entity_type 分布:")
    for t, c in sorted(type_after.items(), key=lambda x: -x[1]):
        print(f"  {t:<25s} {c:>6d}")

    if not args.apply:
        print(f"\n[预览模式] 使用 --apply 标志实际写入文件")
        return

    # 重新生成 global_entity_index.json
    print("\n重新生成 global_entity_index.json ...")
    idx = OrderedDict()
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            continue
        for ent in doc.get("entities", []):
            name = ent.get("name", "").strip()
            eid = ent.get("id", "")
            if not name or not eid:
                continue
            row = {
                "id": eid,
                "file": os.path.basename(fp),
                "type": ent.get("entity_type", "unknown"),
            }
            desc = _truncate_desc(ent.get("description") or "")
            if desc:
                row["description"] = desc
            idx[name] = row

    idx_sorted = OrderedDict(sorted(idx.items()))
    idx_doc = OrderedDict([
        ("_schema", "global_entity_index_v4.0_refined"),
        ("_generated_at", datetime.now(timezone.utc).isoformat()),
        ("total_unique_entities", len(idx_sorted)),
        ("entities", idx_sorted),
    ])
    idx_path = os.path.join(OUT_DIR, "global_entity_index.json")
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(idx_doc, f, ensure_ascii=False, indent=2)
    print(f"  已写入: {idx_path} ({len(idx_sorted)} 实体)")

    # 更新 _refinement_report.json 的 entity_types
    report_path = os.path.join(OUT_DIR, "_refinement_report.json")
    if os.path.exists(report_path):
        print("更新 _refinement_report.json ...")
        with open(report_path, "r", encoding="utf-8") as f:
            report = json.load(f)
        report["graph_stats"]["entity_types"] = dict(
            sorted(type_after.items(), key=lambda x: -x[1])
        )
        report["graph_stats"]["total_entities"] = total_entities
        report["_type_cleanup_at"] = datetime.now(timezone.utc).isoformat()
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"  已更新: {report_path}")

    print("\n清洗完成。")


if __name__ == "__main__":
    main()
