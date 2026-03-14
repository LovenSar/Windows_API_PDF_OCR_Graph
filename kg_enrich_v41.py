#!/usr/bin/env python3
"""
KG v4.1: 结构化类型清洗 + 基于参数/返回值的自动建边

使用方法（建议用 tmux 跑）：

  cd ~/.openclaw/workspace
  TMUX_SESSION=windows_api_kg_v41 TMUX_TIMEOUT=600 \
    ./tmux_exec.sh "cd ~/workspace/Windows_API_PDF && python kg_enrich_v41.py"
"""

import json
import os
import re
import glob
from collections import defaultdict
from copy import deepcopy

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")

ALLOWED_ENTITY_TYPES = {
    "function", "structure", "struct", "enum", "callback", "macro",
    "constant", "typedef", "union", "interface", "ioctl", "event",
    "method", "property", "notification", "oid", "enum_value",
    "error_code", "parameter", "application", "enum_member",
    "function_pointer", "flags", "structure_member", "field", "message",
    "technology", "attribute", "unknown",
}

# 视为“类型节点”的 entity_type，用于 function → type 建边
TYPE_NODE_KINDS = {
    "structure", "struct", "enum", "enum_value", "union",
    "typedef", "constant", "macro", "flags", "error_code",
}

C_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# 一些应当忽略的 C 关键字/常见类型前缀
C_KEYWORDS = {
    "const", "volatile", "signed", "unsigned", "struct", "enum",
    "union", "class", "typedef", "static", "extern", "inline",
    "__in", "__out", "__inout", "_In_", "_Out_", "_Inout_",
}

POINTER_TRIM_RE = re.compile(r"[\s\*]+")


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def normalize_entity_type(et: str) -> str:
    """将 entity_type 归一到合法集合；异常值标记为 unknown。"""
    if not et:
        return "unknown"
    et = str(et).strip()
    lower = et.lower()
    if lower in ("struct", "structure"):
        return "structure"
    if lower in ("enumvalue", "enum_value"):
        return "enum_value"
    if lower not in ALLOWED_ENTITY_TYPES:
        return "unknown"
    return lower


def clean_entity_types_for_doc(doc: dict) -> dict:
    doc = deepcopy(doc)
    for ent in doc.get("entities", []):
        raw = ent.get("entity_type")
        norm = normalize_entity_type(raw)
        if raw != norm:
            if raw is not None:
                ent.setdefault("_type_raw", raw)
            ent["entity_type"] = norm
    return doc


def build_global_index() -> dict:
    """从 json_output_v4/*.json 构建 name -> {id, type} 映射。"""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )
    name_to_info = defaultdict(list)
    entity_list = []

    for fp in files:
        doc = load_json(fp)
        doc = clean_entity_types_for_doc(doc)
        for ent in doc.get("entities", []):
            eid = ent.get("id")
            name = ent.get("name")
            if not eid or not name:
                continue
            et = normalize_entity_type(ent.get("entity_type"))
            info = {"id": eid, "name": name, "entity_type": et}
            entity_list.append(info)
            name_to_info[name].append(info)

    return {"entities": entity_list, "name_to_info": name_to_info}


def tokenize_type_string(ts: str):
    """从类型字符串里抽取候选类型名（粗粒度）。"""
    if not ts:
        return []
    ts = POINTER_TRIM_RE.sub(" ", ts).strip()
    tokens = []
    for part in ts.replace(",", " ").split():
        p = part.strip().strip("()[]{};")
        if not p or p.lower() in C_KEYWORDS:
            continue
        if C_IDENTIFIER_RE.match(p):
            tokens.append(p)
    return tokens


def build_existing_edge_set(edges):
    s = set()
    for e in edges:
        src = e.get("source") or e.get("from")
        tgt = e.get("target") or e.get("to")
        et = e.get("type") or e.get("edge_type") or "unknown"
        if not src or not tgt:
            continue
        s.add((src, tgt, et))
        s.add((tgt, src, et))
    return s


def enrich_edges(name_to_info, edges):
    """基于 parameters/return_value 为函数节点追加类型边。"""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    existing = build_existing_edge_set(edges)
    new_edges = []

    def find_type_targets(type_name: str):
        out = []
        for info in name_to_info.get(type_name, []):
            if info["entity_type"] in TYPE_NODE_KINDS:
                out.append(info["name"])
        return out

    for fp in files:
        doc = load_json(fp)
        source_file = os.path.basename(fp)
        for ent in doc.get("entities", []):
            et = normalize_entity_type(ent.get("entity_type"))
            if et != "function":
                continue
            fname = ent.get("name")
            if not fname:
                continue

            # 参数类型 → parameter_type / uses
            for p in ent.get("parameters") or []:
                ptype = p.get("type")
                if not isinstance(ptype, str):
                    continue
                for token in tokenize_type_string(ptype):
                    for tgt_name in find_type_targets(token):
                        for edge_type in ("parameter_type", "uses"):
                            key = (fname, tgt_name, edge_type)
                            if key in existing:
                                continue
                            edges.append({
                                "source": fname,
                                "target": tgt_name,
                                "type": edge_type,
                                "source_file": source_file,
                                "_v41": True,
                            })
                            existing.add(key)
                            existing.add((tgt_name, fname, edge_type))
                            new_edges.append(edges[-1])

            # 返回值类型 → return_type / returns
            rv = ent.get("return_value")
            if isinstance(rv, dict):
                rv_type = rv.get("type")
            elif isinstance(rv, str):
                rv_type = rv
            else:
                rv_type = None

            if isinstance(rv_type, str):
                for token in tokenize_type_string(rv_type):
                    for tgt_name in find_type_targets(token):
                        for edge_type in ("return_type", "returns"):
                            key = (fname, tgt_name, edge_type)
                            if key in existing:
                                continue
                            edges.append({
                                "source": fname,
                                "target": tgt_name,
                                "type": edge_type,
                                "source_file": source_file,
                                "_v41": True,
                            })
                            existing.add(key)
                            existing.add((tgt_name, fname, edge_type))
                            new_edges.append(edges[-1])

    return new_edges


def main():
    if not os.path.isdir(OUT_DIR):
        raise SystemExit(f"json_output_v4 目录不存在: {OUT_DIR}")

    print("[v4.1] 构建全局索引并清洗 entity_type ...")
    idx = build_global_index()
    entities = idx["entities"]
    name_to_info = idx["name_to_info"]

    idx_path = os.path.join(OUT_DIR, "global_entity_index_v41.json")
    save_json(idx_path, {"entities": entities})
    print(f"[v4.1] 已写入: {idx_path} (实体 {len(entities)} 个)")

    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    if os.path.exists(edges_path):
        edoc = load_json(edges_path)
        edges = edoc.get("edges") or edoc.get("data", {}).get("edges", [])
    else:
        edges = []

    print(f"[v4.1] 现有边数量: {len(edges)}")

    print("[v4.1] 基于参数/返回值自动建边 ...")
    new_edges = enrich_edges(name_to_info, edges)
    print(f"[v4.1] 新增边数量: {len(new_edges)}")

    out_edges_path = os.path.join(OUT_DIR, "global_edges_v41.json")
    save_json(out_edges_path, {"edges": edges})
    print(f"[v4.1] 已写入: {out_edges_path} (总边 {len(edges)})")


if __name__ == "__main__":
    main()
