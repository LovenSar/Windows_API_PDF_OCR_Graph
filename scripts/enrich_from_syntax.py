#!/usr/bin/env python3
"""
从 syntax 字段回填参数类型 + 返回值类型 + header/domain 层次边

解决的问题:
  - 86% 参数缺失 type 字段 → 从 syntax 中解析 C 函数签名回填
  - return_value 多为纯文本 → 从 syntax 提取返回类型
  - 缺少 header 归属边 → 从 requirements.header 提取
  - 缺少 domain 归属边 → 从源文件名提取

用法:
  python enrich_from_syntax.py           # 预览
  python enrich_from_syntax.py --apply   # 实际写入
"""

import json
import os
import re
import glob
import argparse
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")

# ── Syntax 解析 ──────────────────────────────────────────────────

_SAL_RE = re.compile(
    r"(?:_In_|_Out_|_Inout_|_In_opt_|_Out_opt_|_Inout_opt_|"
    r"_Reserved_|_Frees_ptr_opt_|_Outptr_|_Outptr_opt_|"
    r"_In_reads_bytes_\([^)]*\)|_In_reads_\([^)]*\)|"
    r"_Out_writes_bytes_\([^)]*\)|_Out_writes_\([^)]*\)|"
    r"_Out_writes_bytes_opt_\([^)]*\)|_Inout_updates_\([^)]*\)|"
    r"__in|__out|__inout|__in_opt|__out_opt|"
    r"__drv_aliasesMem|__deref_ecount\([^)]*\)|"
    r"\[in\]|\[out\]|\[in,\s*out\]|\[in,\s*optional\]|"
    r"\[out,\s*optional\]|\[in,\s*out,\s*optional\])"
)

_CALLING_CONV_RE = re.compile(
    r"\b(?:WINAPI|APIENTRY|CALLBACK|STDCALL|CDECL|PASCAL|"
    r"FORCEINLINE|NTSYSAPI|NTAPI|__stdcall|__cdecl|__fastcall|"
    r"VKAPI_ATTR|VKAPI_CALL|extern)\b"
)

_C_NOISE_TOKENS = {
    "const", "volatile", "unsigned", "signed", "struct", "enum",
    "union", "register", "static", "inline", "virtual",
}


def normalize_syntax(syn):
    """Flatten multi-line syntax to single string."""
    if not syn:
        return ""
    s = str(syn).replace("\\n", "\n")
    s = re.sub(r"\r\n?", "\n", s)
    lines = [l.strip() for l in s.split("\n") if l.strip()]
    return " ".join(lines)


def parse_signature(syntax_str):
    """Parse a C/C++ function signature into return_type, name, and param list.

    Returns (return_type, func_name, [(type, name), ...]) or None.
    """
    s = normalize_syntax(syntax_str)
    if not s:
        return None

    s = _CALLING_CONV_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    m = re.match(r"^(.+?)\(\s*(.*?)\s*\)\s*;?\s*$", s, re.S)
    if not m:
        return None

    head = m.group(1).strip()
    params_str = m.group(2).strip()

    head_tokens = head.rsplit(None, 1)
    if len(head_tokens) == 2:
        ret_type, func_name = head_tokens
    elif len(head_tokens) == 1:
        ret_type, func_name = "void", head_tokens[0]
    else:
        return None

    ret_type = ret_type.strip().rstrip("*").strip() or "void"
    func_name = func_name.strip().lstrip("*").strip()
    if not re.match(r"^[A-Za-z_]\w*$", func_name):
        return None

    if params_str in ("", "void", "VOID"):
        return (ret_type, func_name, [])

    params = []
    for chunk in re.split(r",(?![^(]*\))", params_str):
        chunk = chunk.strip()
        if chunk == "...":
            params.append(("...", "..."))
            continue

        chunk = _SAL_RE.sub(" ", chunk)
        chunk = re.sub(r"\s+", " ", chunk).strip()
        if not chunk:
            continue

        tokens = chunk.split()
        if len(tokens) >= 2:
            pname = tokens[-1].strip("*&[]")
            ptype = " ".join(tokens[:-1])
            ptype = re.sub(r"\s*\*+", "*", ptype).strip()
            if re.match(r"^[A-Za-z_]\w*$", pname):
                params.append((ptype, pname))
            else:
                params.append((chunk, ""))
        elif len(tokens) == 1:
            params.append((tokens[0], ""))

    return (ret_type, func_name, params)


def extract_header_from_requirements(req):
    """Extract header filename from requirements field."""
    if not req:
        return None
    if isinstance(req, dict):
        h = req.get("header")
        if h:
            return h.strip()
    elif isinstance(req, str):
        m = re.search(r"(\w+\.h)", req, re.I)
        if m:
            return m.group(1)
    return None


def extract_domain_from_filename(source_file):
    """Extract API domain from source JSON filename.

    Filenames like: hardware-drivers-ddi-_acpi_20260305_0120.json
                    win32-api-_gdi_20260305_0148.json
    """
    m = re.match(
        r"(?:hardware-drivers-(?:ddi-)?|win32-api-|win32-)_?(\w+?)_\d{8}",
        source_file,
    )
    if m:
        return m.group(1).replace("_", "-")
    return None


def main():
    parser = argparse.ArgumentParser(description="Syntax-based type backfill + hierarchy edges")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    print(f"扫描 {len(files)} 个实体文件...\n")

    # Stats
    param_backfilled = 0
    rv_backfilled = 0
    parse_ok = 0
    parse_fail = 0
    header_edges = []
    domain_edges = []
    all_entity_names = set()
    type_entity_names = set()
    files_modified = 0

    # First pass: collect all entity names and type entities
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
        for ent in doc.get("entities", []):
            name = ent.get("name", "").strip()
            if name:
                all_entity_names.add(name)
                et = str(ent.get("entity_type", "")).strip().lower()
                if et in ("structure", "enum", "typedef", "union",
                          "callback", "flags", "enum_value", "interface", "class"):
                    type_entity_names.add(name)

    type_name_lower = {n.lower(): n for n in type_entity_names}

    # Second pass: parse syntax, backfill, collect edges
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
        source_file = os.path.basename(fp)
        domain = extract_domain_from_filename(source_file)
        modified = False

        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "")).strip().lower()
            name = ent.get("name", "").strip()
            if not name:
                continue

            # Header edge
            header = extract_header_from_requirements(ent.get("requirements"))
            if header:
                header_edges.append({
                    "source": name,
                    "target": header,
                    "type": "belongs_to_header",
                    "source_file": source_file,
                })

            # Domain edge
            if domain:
                domain_edges.append({
                    "source": name,
                    "target": f"domain:{domain}",
                    "type": "belongs_to_domain",
                    "source_file": source_file,
                })

            if et not in ("function", "callback", "method"):
                continue

            syn = ent.get("syntax")
            if not syn:
                continue

            result = parse_signature(syn)
            if not result:
                parse_fail += 1
                continue
            parse_ok += 1

            ret_type, parsed_name, parsed_params = result

            # Backfill return value type
            rv = ent.get("return_value")
            if ret_type and ret_type.lower() != "void":
                if rv is None:
                    ent["return_value"] = {"type": ret_type, "description": ""}
                    rv_backfilled += 1
                    modified = True
                elif isinstance(rv, dict) and not rv.get("type"):
                    rv["type"] = ret_type
                    rv_backfilled += 1
                    modified = True
                elif isinstance(rv, str):
                    ent["return_value"] = {"type": ret_type, "description": rv}
                    rv_backfilled += 1
                    modified = True

            # Backfill parameter types
            params = ent.get("parameters") or []
            if not params:
                continue

            sig_map = {}
            for ptype, pname in parsed_params:
                if pname and pname != "...":
                    sig_map[pname.lower()] = ptype

            for p in params:
                if not isinstance(p, dict):
                    continue
                if p.get("type"):
                    continue
                pname = p.get("name", "")
                matched_type = sig_map.get(pname.lower())
                if matched_type:
                    p["type"] = matched_type
                    param_backfilled += 1
                    modified = True

        if modified and args.apply:
            tmp = fp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            os.replace(tmp, fp)
            files_modified += 1

    # Deduplicate header edges
    seen_header = set()
    unique_header = []
    for e in header_edges:
        k = (e["source"], e["target"])
        if k not in seen_header:
            seen_header.add(k)
            unique_header.append(e)
    header_edges = unique_header

    seen_domain = set()
    unique_domain = []
    for e in domain_edges:
        k = (e["source"], e["target"])
        if k not in seen_domain:
            seen_domain.add(k)
            unique_domain.append(e)
    domain_edges = unique_domain

    # Generate signature edges from enriched data
    sig_edges = []
    if args.apply:
        sig_edges = _generate_signature_edges(files, type_name_lower)

    print("=" * 70)
    print("增强结果汇总")
    print("=" * 70)
    print(f"  语法解析成功: {parse_ok}")
    print(f"  语法解析失败: {parse_fail}")
    print(f"  参数 type 回填: {param_backfilled}")
    print(f"  返回值 type 回填: {rv_backfilled}")
    print(f"  修改文件数: {files_modified if args.apply else '(预览)'}")
    print()
    print(f"  header 边: {len(header_edges)} (去重后)")
    print(f"  domain 边: {len(unique_domain)} (去重后)")
    if sig_edges:
        print(f"  新签名边: {len(sig_edges)}")
    print()

    if not args.apply:
        print("[预览模式] 使用 --apply 写入")
        return

    # Merge new edges into global_edges.json
    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    with open(edges_path, "r", encoding="utf-8") as f:
        edge_doc = json.load(f)
    existing_edges = edge_doc.get("edges", [])

    existing_keys = set()
    for e in existing_edges:
        existing_keys.add((e.get("source", ""), e.get("target", ""), e.get("type", "")))

    added = 0
    for new_e in header_edges + domain_edges + sig_edges:
        k = (new_e["source"], new_e["target"], new_e["type"])
        if k not in existing_keys:
            existing_edges.append(new_e)
            existing_keys.add(k)
            added += 1

    edge_doc["edges"] = existing_edges
    edge_doc["total_edges"] = len(existing_edges)
    edge_doc["_schema"] = "api_edges_v4.4_enriched"
    with open(edges_path, "w", encoding="utf-8") as f:
        json.dump(edge_doc, f, ensure_ascii=False, indent=2)
    print(f"已更新 global_edges.json: 新增 {added} 条边, 总计 {len(existing_edges)} 条")


def _generate_signature_edges(files, type_name_lower):
    """Re-generate signature edges from backfilled param/return types."""
    edges = []
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
        source_file = os.path.basename(fp)
        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "")).strip().lower()
            if et not in ("function", "callback", "method"):
                continue
            func_name = ent.get("name", "").strip()
            if not func_name:
                continue

            type_tokens = set()
            for param in ent.get("parameters") or []:
                if isinstance(param, dict):
                    pt = param.get("type", "")
                    if pt:
                        for tok in re.findall(r"[A-Za-z_]\w{2,}", str(pt)):
                            if tok.lower() not in _C_NOISE_TOKENS:
                                type_tokens.add(tok)
                                m = re.match(r"^(?:LP|PC|PPC|PP|P)([A-Z]\w+)$", tok)
                                if m:
                                    type_tokens.add(m.group(1))

            rv = ent.get("return_value")
            if isinstance(rv, dict):
                rt = rv.get("type", "")
            elif isinstance(rv, str):
                rt = rv.split()[0] if rv.strip() else ""
            else:
                rt = ""
            if rt:
                for tok in re.findall(r"[A-Za-z_]\w{2,}", str(rt)):
                    if tok.lower() not in _C_NOISE_TOKENS:
                        type_tokens.add(tok)

            for tok in type_tokens:
                target = type_name_lower.get(tok.lower())
                if target and target != func_name:
                    edges.append({
                        "source": func_name,
                        "target": target,
                        "type": "uses_type",
                        "source_file": source_file,
                        "_enriched_by": "syntax_backfill",
                    })

    # Deduplicate
    seen = set()
    unique = []
    for e in edges:
        k = (e["source"], e["target"], e["type"])
        if k not in seen:
            seen.add(k)
            unique.append(e)
    return unique


if __name__ == "__main__":
    main()
