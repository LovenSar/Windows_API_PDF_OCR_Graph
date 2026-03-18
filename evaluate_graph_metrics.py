import argparse
import glob
import hashlib
import json
import math
import os
import random
import re
import statistics
import time
import urllib.error
import urllib.request
from collections import defaultdict

import networkx as nx


SECTION_TITLES = {
    "函数", "结构", "枚举", "回调", "接口", "方法", "宏", "常量", "联合", "属性", "类", "事件",
    "functions", "structures", "enums", "callbacks", "interfaces", "methods", "macros", "constants",
}


def normalize_name(value):
    if value is None:
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def safe_float(value, default_value=0.0):
    try:
        return float(value)
    except Exception:
        return default_value


def load_json(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def load_alias_table(path):
    if not path or not os.path.exists(path):
        return {}
    data = load_json(path)
    mapping = defaultdict(set)
    canonical_to_aliases = defaultdict(set)
    alias_to_canonicals = defaultdict(set)
    if not isinstance(data, dict):
        return {}

    for key, raw_values in data.items():
        canonical = normalize_name(key).lower()
        if not canonical:
            continue
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for item in values:
            alias = normalize_name(item).lower()
            if not alias:
                continue
            canonical_to_aliases[canonical].add(alias)
            alias_to_canonicals[alias].add(canonical)

    for canonical, aliases in canonical_to_aliases.items():
        mapping[canonical].update(aliases)

    # 仅当别名唯一归属时建立反向映射，避免同名别名冲突污染。
    for alias, canonicals in alias_to_canonicals.items():
        if len(canonicals) == 1:
            owner = next(iter(canonicals))
            mapping[alias].add(owner)

    return {k: sorted(v) for k, v in mapping.items()}


def default_meta_path_profiles():
    return {
        "default": {
            "relation_weights": {
                "cross_reference": 1.0,
                "returns_type": 1.15,
                "has_parameter": 1.1,
                "related_to": 0.8,
                "*": 0.7,
            },
            "transitions": {
                "*": ["cross_reference", "returns_type", "has_parameter", "related_to"],
                "returns_type": ["cross_reference", "has_parameter", "related_to"],
                "has_parameter": ["cross_reference", "returns_type", "has_parameter", "related_to"],
                "cross_reference": ["cross_reference", "returns_type", "has_parameter", "related_to"],
            },
        },
        "win32-api": {
            "relation_weights": {
                "cross_reference": 1.1,
                "returns_type": 1.3,
                "has_parameter": 1.25,
                "related_to": 0.6,
                "*": 0.7,
            },
            "transitions": {
                "*": ["cross_reference", "returns_type", "has_parameter", "related_to"],
                "has_parameter": ["has_parameter", "returns_type", "cross_reference"],
                "returns_type": ["cross_reference", "has_parameter"],
                "cross_reference": ["cross_reference", "returns_type", "has_parameter"],
            },
        },
        "hardware-drivers": {
            "relation_weights": {
                "cross_reference": 1.25,
                "returns_type": 1.0,
                "has_parameter": 1.2,
                "related_to": 0.75,
                "*": 0.7,
            },
            "transitions": {
                "*": ["cross_reference", "returns_type", "has_parameter", "related_to"],
                "cross_reference": ["cross_reference", "has_parameter", "returns_type"],
                "has_parameter": ["cross_reference", "has_parameter", "returns_type"],
                "returns_type": ["cross_reference", "related_to"],
            },
        },
    }


def load_meta_path_policy(path, profile):
    profiles = default_meta_path_profiles()

    if path and os.path.exists(path):
        raw = load_json(path)
        if isinstance(raw, dict):
            raw_profiles = raw.get("profiles", raw)
            if isinstance(raw_profiles, dict):
                for p_name, p_conf in raw_profiles.items():
                    if not isinstance(p_conf, dict):
                        continue
                    if p_name not in profiles:
                        profiles[p_name] = {}
                    current = profiles[p_name]
                    current_weights = current.get("relation_weights", {}) if isinstance(current.get("relation_weights", {}), dict) else {}
                    current_transitions = current.get("transitions", {}) if isinstance(current.get("transitions", {}), dict) else {}

                    incoming_weights = p_conf.get("relation_weights", {}) if isinstance(p_conf.get("relation_weights", {}), dict) else {}
                    incoming_transitions = p_conf.get("transitions", {}) if isinstance(p_conf.get("transitions", {}), dict) else {}

                    merged_weights = dict(current_weights)
                    merged_weights.update(incoming_weights)
                    merged_transitions = dict(current_transitions)
                    merged_transitions.update(incoming_transitions)
                    profiles[p_name] = {
                        "relation_weights": merged_weights,
                        "transitions": merged_transitions,
                    }

    picked = profiles.get(profile) or profiles.get("default") or {}
    relation_weights = picked.get("relation_weights", {}) if isinstance(picked, dict) else {}
    transitions = picked.get("transitions", {}) if isinstance(picked, dict) else {}

    if not isinstance(relation_weights, dict):
        relation_weights = {}
    if not isinstance(transitions, dict):
        transitions = {}

    normalized_weights = {}
    for key, value in relation_weights.items():
        k = normalize_name(key).lower() or "*"
        normalized_weights[k] = max(0.0, safe_float(value, 1.0))
    if "*" not in normalized_weights:
        normalized_weights["*"] = 1.0

    normalized_transitions = {}
    for src_rel, dst_list in transitions.items():
        src = normalize_name(src_rel).lower() or "*"
        if isinstance(dst_list, list):
            values = {normalize_name(x).lower() for x in dst_list if normalize_name(x)}
        else:
            values = set()
        if values:
            normalized_transitions[src] = values

    if "*" not in normalized_transitions:
        normalized_transitions["*"] = {"cross_reference", "returns_type", "has_parameter", "related_to"}

    return {
        "requested_profile": profile,
        "profile": profile if profile in profiles else "default",
        "available_profiles": sorted(profiles.keys()),
        "relation_weights": normalized_weights,
        "transitions": normalized_transitions,
    }


def init_gt_templates(template_dir):
    os.makedirs(template_dir, exist_ok=True)

    nodes_template = {
        "description": "Ground Truth nodes template",
        "nodes": [
            {"id": "windows::CreateFileW", "name": "CreateFileW"},
            {"id": "windows::HANDLE", "name": "HANDLE"}
        ]
    }

    edges_template = {
        "description": "Ground Truth edges template",
        "edges": [
            {"source": "windows::CreateFileW", "target": "windows::HANDLE", "type": "returns_type"}
        ]
    }

    layout_template = {
        "description": "Ground Truth layout template",
        "documents": [
            {
                "doc_id": "win32-api::fileapi",
                "reading_order": ["CreateFileW", "ReadFile", "WriteFile"],
                "sections": [
                    {"title": "Functions", "entities": ["CreateFileW", "ReadFile", "WriteFile"]},
                    {"title": "Structures", "entities": ["OVERLAPPED"]}
                ],
                "table_edges": [
                    {"header": "name", "cell": "lpFileName"},
                    {"header": "type", "cell": "LPCWSTR"}
                ]
            }
        ]
    }

    query_pairs_template = {
        "description": "Ground Truth semantic query pairs template",
        "query_pairs": [
            {"source": "windows::CreateFileW", "target": "windows::CloseHandle"},
            {"source": "CreateFileW", "target": "HANDLE"}
        ]
    }

    with open(os.path.join(template_dir, "gt_nodes_template.json"), "w", encoding="utf-8") as f:
        json.dump(nodes_template, f, ensure_ascii=False, indent=2)
    with open(os.path.join(template_dir, "gt_edges_template.json"), "w", encoding="utf-8") as f:
        json.dump(edges_template, f, ensure_ascii=False, indent=2)
    with open(os.path.join(template_dir, "gt_layout_template.json"), "w", encoding="utf-8") as f:
        json.dump(layout_template, f, ensure_ascii=False, indent=2)
    with open(os.path.join(template_dir, "gt_query_pairs_template.json"), "w", encoding="utf-8") as f:
        json.dump(query_pairs_template, f, ensure_ascii=False, indent=2)


def load_layout_gt(path):
    if not path or not os.path.exists(path):
        return {}
    data = load_json(path)
    documents = []
    if isinstance(data, dict):
        documents = data.get("documents", [])
    elif isinstance(data, list):
        documents = data

    mapping = {}
    for item in documents:
        if not isinstance(item, dict):
            continue
        doc_id = normalize_name(item.get("doc_id"))
        if not doc_id:
            continue
        reading_order = item.get("reading_order", [])
        sections = item.get("sections", [])
        table_edges = item.get("table_edges", [])
        mapping[doc_id] = {
            "reading_order": [normalize_name(x) for x in reading_order if normalize_name(x)],
            "sections": sections if isinstance(sections, list) else [],
            "table_edges": table_edges if isinstance(table_edges, list) else [],
        }
    return mapping


def build_gt_tree_from_layout_sections(sections):
    root = TreeNode("ROOT")
    if not isinstance(sections, list) or not sections:
        return root
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        title = normalize_name(sec.get("title", "Section"))
        section_node = root.add(TreeNode(f"SEC::{title}"))
        for name in sec.get("entities", []) or []:
            norm_name = normalize_name(name)
            if norm_name:
                section_node.add(TreeNode(f"ENT::{norm_name}"))
    return root


def parse_query_pairs(raw):
    pairs = []
    if isinstance(raw, dict):
        source = raw.get("query_pairs", [])
    elif isinstance(raw, list):
        source = raw
    else:
        source = []

    for item in source:
        if isinstance(item, dict):
            src = normalize_name(item.get("source"))
            tgt = normalize_name(item.get("target"))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            src = normalize_name(item[0])
            tgt = normalize_name(item[1])
        else:
            continue
        if src and tgt:
            pairs.append((src, tgt))
    return pairs


def locate_source_file(workspace_dir, source_file):
    if not source_file:
        return None
    p1 = os.path.join(workspace_dir, source_file)
    if os.path.exists(p1):
        return p1
    p2 = os.path.join(workspace_dir, "OCR_raw", source_file)
    if os.path.exists(p2):
        return p2
    return None


def read_text_file(path):
    if not path or not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="ignore") as file:
        return file.read()


def parse_entities_and_edges(json_pattern, workspace_dir):
    files = sorted(glob.glob(json_pattern))
    entities = {}
    entity_to_doc = {}
    doc_meta = {}
    edges = set()
    explicit_nodes = set()
    edge_confidence = {}

    for fp in files:
        try:
            data = load_json(fp)
        except Exception as exc:
            print(f"[WARN] 读取失败: {fp} ({exc})")
            continue

        document = data.get("document", {})
        doc_id = document.get("document_id", os.path.basename(fp))
        source_file = document.get("source_file", "")
        source_path = locate_source_file(workspace_dir, source_file)
        doc_meta[doc_id] = {
            "json_file": fp,
            "source_file": source_file,
            "source_path": source_path,
            "domain": document.get("domain", ""),
            "topic": document.get("topic", ""),
        }

        local_conf = {}
        local_name_to_id = {}
        accepted_ids = set()
        for entity in data.get("entities", []):
            eid = normalize_name(entity.get("id"))
            if not eid:
                continue

            if eid in entities:
                # 避免跨文档同 ID 覆盖，保留首个版本。
                continue

            name = normalize_name(entity.get("name"))
            etype = normalize_name(entity.get("entity_type", "unknown")).lower() or "unknown"
            confidence = safe_float(entity.get("confidence", 0.0))
            source_line = entity.get("_source_line")

            merged = dict(entity)
            merged["name"] = name
            merged["entity_type"] = etype
            merged["confidence"] = confidence
            merged["_source_line"] = source_line
            merged["_doc_id"] = doc_id

            entities[eid] = merged
            explicit_nodes.add(eid)
            entity_to_doc[eid] = doc_id
            accepted_ids.add(eid)
            local_conf[eid] = confidence
            if name:
                local_name_to_id[name] = eid

        for entity in data.get("entities", []):
            sid = normalize_name(entity.get("id"))
            if not sid:
                continue
            if sid not in accepted_ids:
                # 该 ID 在本轮未被注册（通常是跨文档重复 ID），跳过其产边，避免污染。
                continue

            src_conf = safe_float(entity.get("confidence", 0.0))

            for ref in entity.get("cross_references", []) or []:
                tid = normalize_name(ref)
                if not tid:
                    continue
                edge = (sid, tid, "cross_reference")
                edges.add(edge)
                edge_confidence[edge] = src_conf

            for p in entity.get("parameters", []) or []:
                ptype = normalize_name((p or {}).get("type"))
                if not ptype:
                    continue
                edge = (sid, ptype, "has_parameter")
                edges.add(edge)
                edge_confidence[edge] = src_conf

            return_type = normalize_name(entity.get("return_type"))
            if not return_type:
                rv = entity.get("return_value", {})
                if isinstance(rv, dict):
                    return_type = normalize_name(rv.get("type"))

            if return_type and return_type.lower() != "void":
                edge = (sid, return_type, "returns_type")
                edges.add(edge)
                edge_confidence[edge] = src_conf

        for rel in data.get("relationships", []) or []:
            source = normalize_name(rel.get("source"))
            target = normalize_name(rel.get("target"))
            rtype = normalize_name(rel.get("relation_type", rel.get("type", "related_to"))).lower()
            if not source or not target:
                continue
            source_mapped = local_name_to_id.get(source, source)
            target_mapped = local_name_to_id.get(target, target)
            edge = (source_mapped, target_mapped, rtype)
            edges.add(edge)
            rel_conf = safe_float(rel.get("confidence", local_conf.get(source_mapped, 0.5)), 0.5)
            edge_confidence[edge] = rel_conf

    # 统一把可解析的名称端点映射到显式实体 ID，提升图连通性。
    name_to_id = {}
    for eid, entity in entities.items():
        name_to_id[eid] = eid
        name = normalize_name(entity.get("name"))
        if name and name not in name_to_id:
            name_to_id[name] = eid

    remapped_edges = set()
    remapped_confidence = {}
    for source, target, relation in sorted(edges):
        rs = name_to_id.get(source, source)
        rt = name_to_id.get(target, target)
        new_edge = (rs, rt, relation)
        remapped_edges.add(new_edge)
        conf = edge_confidence.get((source, target, relation), 0.5)
        remapped_confidence[new_edge] = max(remapped_confidence.get(new_edge, 0.0), conf)

    # [v4.2] 加载并合并 global_edges.json（来自孤立节点补救）
    global_edges_path = os.path.join(workspace_dir, "json_output_v4", "global_edges.json")
    if os.path.exists(global_edges_path):
        try:
            global_edges_data = load_json(global_edges_path)
            global_edges_list = global_edges_data.get("edges", [])
            merged_count = 0
            for edge_item in global_edges_list:
                source = normalize_name(edge_item.get("source"))
                target = normalize_name(edge_item.get("target"))
                edge_type = normalize_name(edge_item.get("type", edge_item.get("edge_type", "related_to"))).lower()
                
                if not source or not target:
                    continue
                
                rs = name_to_id.get(source, source)
                rt = name_to_id.get(target, target)
                new_edge = (rs, rt, edge_type)
                
                if new_edge not in remapped_edges:
                    remapped_edges.add(new_edge)
                    conf = safe_float(edge_item.get("confidence", 0.5), 0.5)
                    remapped_confidence[new_edge] = conf
                    merged_count += 1
            
            if merged_count > 0:
                print(f"[INFO] 合并 global_edges.json: 新增 {merged_count} 条边")
        except Exception as exc:
            print(f"[WARN] 合并 global_edges.json 失败: {exc}")

    return entities, explicit_nodes, remapped_edges, remapped_confidence, doc_meta, entity_to_doc


def build_graph(explicit_nodes, edges, entities):
    graph = nx.DiGraph()
    for node in explicit_nodes:
        entity = entities.get(node, {})
        graph.add_node(
            node,
            is_explicit=True,
            entity_type=entity.get("entity_type", "unknown"),
            name=entity.get("name", ""),
            confidence=safe_float(entity.get("confidence", 0.0)),
        )
    for source, target, relation in edges:
        if not graph.has_node(source):
            graph.add_node(source, is_explicit=False, entity_type="implicit", name=source)
        if not graph.has_node(target):
            graph.add_node(target, is_explicit=False, entity_type="implicit", name=target)
        graph.add_edge(source, target, relation=relation)
    return graph


def extract_ocr_entity_candidates(text):
    candidates = set()
    # 全大写宏 / IOCTL 风格
    for token in re.findall(r"\b[A-Z][A-Z0-9_]{2,}\b", text):
        candidates.add(token)
    # 常见函数命名
    for token in re.findall(r"\b[A-Z][a-zA-Z0-9]{2,}\b", text):
        candidates.add(token)
    # 调用风格
    for token in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", text):
        candidates.add(token)
    return candidates


def first_occurrence_order(text, names, alias_table=None):
    positions = {}
    if not text:
        return positions
    lowered = text.lower()
    alias_table = alias_table or {}

    def has_cjk(value):
        for ch in value:
            if "\u4e00" <= ch <= "\u9fff":
                return True
        return False

    def is_word_char(ch):
        return ch.isalnum() or ch == "_"

    def is_cjk_char(ch):
        return "\u4e00" <= ch <= "\u9fff"

    def is_cjk_boundary_char(ch):
        if not ch:
            return True
        if ch.isspace():
            return True
        if ch in "，。；：、,.!?！？()[]{}<>《》\"'“”‘’/\\|+-=*&#%@~`":
            return True
        return False

    def find_all_with_word_boundary(haystack, needle, max_hits=8):
        matches = []
        start = 0
        n = len(needle)
        h = len(haystack)
        while start < h and len(matches) < max_hits:
            pos = haystack.find(needle, start)
            if pos < 0:
                break
            left_ok = pos == 0 or not is_word_char(haystack[pos - 1])
            right_idx = pos + n
            right_ok = right_idx >= h or not is_word_char(haystack[right_idx])
            if left_ok and right_ok:
                matches.append((pos, pos + n))
            start = pos + 1
        return matches

    def find_all_cjk_with_soft_boundary(haystack, needle, max_hits=8):
        matches = []
        start = 0
        n = len(needle)
        h = len(haystack)
        while start < h and len(matches) < max_hits:
            pos = haystack.find(needle, start)
            if pos < 0:
                break
            left = haystack[pos - 1] if pos > 0 else ""
            right_idx = pos + n
            right = haystack[right_idx] if right_idx < h else ""
            if n <= 2:
                left_ok = is_cjk_boundary_char(left) or not is_cjk_char(left)
                right_ok = is_cjk_boundary_char(right) or not is_cjk_char(right)
            else:
                left_ok = is_cjk_boundary_char(left) or not is_cjk_char(left) or left == ""
                right_ok = is_cjk_boundary_char(right) or not is_cjk_char(right) or right == ""
            if left_ok and right_ok:
                matches.append((pos, pos + n))
            start = pos + 1
        return matches

    def find_all_plain(haystack, needle, max_hits=8):
        matches = []
        start = 0
        n = len(needle)
        h = len(haystack)
        while start < h and len(matches) < max_hits:
            pos = haystack.find(needle, start)
            if pos < 0:
                break
            matches.append((pos, pos + n))
            start = pos + 1
        return matches

    def spans_overlap(a_start, a_end, b_start, b_end):
        return not (a_end <= b_start or b_end <= a_start)

    # 先匹配长实体，并在全局做最长不重叠分配，降低短词误匹配。
    ordered_names = sorted(names, key=lambda x: len(normalize_name(x)), reverse=True)
    all_candidates = []
    for name in ordered_names:
        key = normalize_name(name)
        if not key:
            continue
        key_lower = key.lower()

        variants = [key_lower]
        for alias in alias_table.get(key_lower, []):
            alias_norm = normalize_name(alias).lower()
            if alias_norm and alias_norm not in variants:
                variants.append(alias_norm)

        for variant in variants:
            if has_cjk(variant):
                hits = find_all_cjk_with_soft_boundary(lowered, variant)
            elif re.search(r"[a-zA-Z0-9_]", variant):
                hits = find_all_with_word_boundary(lowered, variant)
            else:
                hits = find_all_plain(lowered, variant)
            for start, end in hits:
                all_candidates.append(
                    {
                        "name": key,
                        "start": start,
                        "end": end,
                        "length": end - start,
                        "is_alias": variant != key_lower,
                    }
                )

    assigned = {}
    used_spans = []
    for cand in sorted(all_candidates, key=lambda x: (x["is_alias"], -x["length"], x["start"])):
        name = cand["name"]
        if name in assigned:
            continue
        overlap = False
        for s, e in used_spans:
            if spans_overlap(cand["start"], cand["end"], s, e):
                overlap = True
                break
        if overlap:
            continue
        assigned[name] = cand["start"]
        used_spans.append((cand["start"], cand["end"]))

    positions.update(assigned)
    return positions


class TreeNode:
    def __init__(self, label):
        self.label = label
        self.children = []

    def add(self, child):
        self.children.append(child)
        return child


def subtree_size(node):
    total = 1
    for child in node.children:
        total += subtree_size(child)
    return total


def ordered_tree_edit_distance(root_a, root_b):
    memo = {}
    size_memo = {}

    def size(node):
        key = id(node)
        if key not in size_memo:
            size_memo[key] = subtree_size(node)
        return size_memo[key]

    def dist(node_a, node_b):
        key = (id(node_a), id(node_b))
        if key in memo:
            return memo[key]

        rename_cost = 0 if node_a.label == node_b.label else 1
        children_a = node_a.children
        children_b = node_b.children
        m = len(children_a)
        n = len(children_b)

        dp = [[0] * (n + 1) for _ in range(m + 1)]
        for i in range(1, m + 1):
            dp[i][0] = dp[i - 1][0] + size(children_a[i - 1])
        for j in range(1, n + 1):
            dp[0][j] = dp[0][j - 1] + size(children_b[j - 1])

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                delete_cost = dp[i - 1][j] + size(children_a[i - 1])
                insert_cost = dp[i][j - 1] + size(children_b[j - 1])
                replace_cost = dp[i - 1][j - 1] + dist(children_a[i - 1], children_b[j - 1])
                dp[i][j] = min(delete_cost, insert_cost, replace_cost)

        value = rename_cost + dp[m][n]
        memo[key] = value
        return value

    return dist(root_a, root_b)


def build_extracted_tree(text, doc_entities):
    root = TreeNode("ROOT")
    if not text:
        fallback = root.add(TreeNode("SEC::flat"))
        for entity in sorted(doc_entities, key=lambda x: (x.get("_source_line") is None, x.get("_source_line", 10**9))):
            label = entity.get("name") or entity.get("id")
            fallback.add(TreeNode(f"ENT::{label}"))
        return root

    section_nodes = {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    section_indices = []
    for idx, line in enumerate(lines):
        low = line.lower()
        if line in SECTION_TITLES or low in SECTION_TITLES:
            key = f"SEC::{line}"
            if key not in section_nodes:
                section_nodes[key] = root.add(TreeNode(key))
            section_indices.append((idx, key))

    if not section_indices:
        section_nodes["SEC::flat"] = root.add(TreeNode("SEC::flat"))
        section_indices.append((0, "SEC::flat"))

    section_indices = sorted(section_indices, key=lambda x: x[0])

    def pick_section(line_number):
        if line_number is None:
            return section_indices[0][1]
        section_key = section_indices[0][1]
        for idx, key in section_indices:
            if idx <= line_number:
                section_key = key
            else:
                break
        return section_key

    for entity in sorted(doc_entities, key=lambda x: (x.get("_source_line") is None, x.get("_source_line", 10**9))):
        label = normalize_name(entity.get("name") or entity.get("id"))
        if not label:
            continue
        line_number = entity.get("_source_line")
        if isinstance(line_number, int):
            line_index = max(line_number - 1, 0)
        else:
            line_index = None
        sec_key = pick_section(line_index)
        section_nodes[sec_key].add(TreeNode(f"ENT::{label}"))

    if not root.children:
        fallback = root.add(TreeNode("SEC::flat"))
        for entity in doc_entities[:200]:
            label = normalize_name(entity.get("name") or entity.get("id"))
            if label:
                fallback.add(TreeNode(f"ENT::{label}"))

    return root


def build_gt_tree_from_text(text, doc_entities):
    root = TreeNode("ROOT")
    if not text:
        return root

    section_nodes = {}
    current = root
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    entity_names = [normalize_name(e.get("name") or e.get("id")) for e in doc_entities]
    entity_names = [n for n in entity_names if n]
    seen_entities = set()

    for line in lines:
        low = line.lower()
        if line in SECTION_TITLES or low in SECTION_TITLES:
            key = f"SEC::{line}"
            if key not in section_nodes:
                section_nodes[key] = root.add(TreeNode(key))
            current = section_nodes[key]
            continue

        for name in entity_names:
            if name in seen_entities:
                continue
            if name.lower() in low:
                current.add(TreeNode(f"ENT::{name}"))
                seen_entities.add(name)
                break

    if not root.children:
        fallback = root.add(TreeNode("SEC::flat"))
        for name in entity_names[:200]:
            fallback.add(TreeNode(f"ENT::{name}"))

    return root


class LLMJudge:
    def __init__(self, llm_config_path):
        raw = load_json(llm_config_path)
        self.api_base_url = normalize_name(raw.get("api_base_url", "")).rstrip("/")
        self.api_key = normalize_name(raw.get("api_key", ""))
        self.model_name = normalize_name(raw.get("model_name", ""))
        self.timeout = int(raw.get("timeout", 120))
        self.requests_per_min = int(raw.get("requests_per_min", 30))
        self.min_interval = 60.0 / max(self.requests_per_min, 1)
        self.last_call_time = 0.0

    def chat_json(self, messages, temperature=0.0, max_tokens=700):
        now = time.time()
        wait = self.min_interval - (now - self.last_call_time)
        if wait > 0:
            time.sleep(wait)
        self.last_call_time = time.time()

        if not (self.api_base_url.startswith("http://") or self.api_base_url.startswith("https://")):
            return None
        url = self.api_base_url + "/chat/completions"
        payload = {
            "model": self.model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            request = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", errors="ignore")
                data = json.loads(body)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, ValueError):
            return None

        choices = data.get("choices", [])
        if not choices:
            return None
        content = choices[0].get("message", {}).get("content", "")
        if not content:
            return None

        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?", "", content, flags=re.IGNORECASE).strip()
            content = re.sub(r"```$", "", content).strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(content[start:end + 1])
                except json.JSONDecodeError:
                    return None
            return None


def compute_topology_metrics(graph, entities, explicit_nodes, edges, doc_meta, gt_nodes_path=None, gt_edges_path=None, expected_density=None):
    results = {}

    explicit_edges = [(s, t, r) for s, t, r in edges if s in explicit_nodes]

    def canonical_node_key(value):
        text = normalize_name(value)
        if text in entities:
            name = normalize_name(entities[text].get("name"))
            return (name or text).lower()
        return text.lower()

    def canonical_edge_key(source, target, relation):
        return (
            canonical_node_key(source),
            canonical_node_key(target),
            normalize_name(relation).lower() or "related_to",
        )

    gt_nodes = None
    gt_edges = None

    if gt_nodes_path and os.path.exists(gt_nodes_path):
        gt_data = load_json(gt_nodes_path)
        if isinstance(gt_data, dict) and "entities" in gt_data:
            gt_nodes = {normalize_name(e.get("id") or e.get("name")) for e in gt_data.get("entities", [])}
        elif isinstance(gt_data, dict) and "entity_index" in gt_data:
            gt_nodes = {normalize_name(k) for k in gt_data.get("entity_index", {}).keys()}
        elif isinstance(gt_data, list):
            gt_nodes = {normalize_name(x) for x in gt_data}
        if gt_nodes is not None:
            gt_nodes = {canonical_node_key(x) for x in gt_nodes if x}

    if gt_edges_path and os.path.exists(gt_edges_path):
        gt_data = load_json(gt_edges_path)
        raw_edges = []
        if isinstance(gt_data, dict) and "edges" in gt_data:
            raw_edges = gt_data.get("edges", [])
        elif isinstance(gt_data, list):
            raw_edges = gt_data
        gt_edges = set()
        for edge in raw_edges:
            if isinstance(edge, dict):
                s = normalize_name(
                    edge.get("source")
                    or edge.get("src")
                    or edge.get("from")
                )
                t = normalize_name(
                    edge.get("target")
                    or edge.get("dst")
                    or edge.get("to")
                )
                r = normalize_name(edge.get("type", edge.get("relation_type", "related_to"))).lower()
            elif isinstance(edge, (list, tuple)) and len(edge) >= 2:
                s = normalize_name(edge[0])
                t = normalize_name(edge[1])
                r = normalize_name(edge[2] if len(edge) >= 3 else "related_to").lower()
            else:
                continue
            if s and t:
                gt_edges.add(canonical_edge_key(s, t, r))

    extracted_node_set = {canonical_node_key(n) for n in explicit_nodes}
    extracted_edge_set = {canonical_edge_key(s, t, r) for s, t, r in explicit_edges}

    # 节点召回率
    if gt_nodes is not None and len(gt_nodes) > 0:
        node_recall = len(extracted_node_set & gt_nodes) / len(gt_nodes)
        node_status = "exact"
    else:
        # 代理 GT：OCR 候选实体集合
        proxy_gt = set()
        for meta in doc_meta.values():
            text = read_text_file(meta.get("source_path"))
            proxy_gt.update(extract_ocr_entity_candidates(text))
        if proxy_gt:
            extracted_names = {canonical_node_key(n) for n in explicit_nodes}
            proxy_gt_norm = {normalize_name(x).lower() for x in proxy_gt if x}
            node_recall = len(extracted_names & proxy_gt_norm) / max(len(proxy_gt_norm), 1)
            node_status = "proxy"
        else:
            node_recall = 0.0
            node_status = "skipped"

    results["node_recall"] = {
        "formula": "Recall_node = |V_ext ∩ V_gt| / |V_gt|",
        "value": node_recall,
        "status": node_status,
    }

    # 边精确率
    if gt_edges is not None and len(gt_edges) > 0:
        edge_precision = len(extracted_edge_set & gt_edges) / max(len(extracted_edge_set), 1)
        edge_status = "exact"
    else:
        edge_precision = None
        edge_status = "not_available_without_gt"

    results["edge_precision"] = {
        "formula": "Precision_edge = |E_ext ∩ E_gt| / |E_ext|",
        "value": edge_precision,
        "status": edge_status,
    }

    results["edge_information_density"] = {
        "formula": "EdgeInfoDensity = |E_ext| / |V_explicit|",
        "value": len(extracted_edge_set) / max(len(explicit_nodes), 1),
        "status": "proxy",
        "note": "无 GT 时不强行报告 precision，改用信息密度作为替代观测量。",
    }

    isolated_explicit = [n for n in explicit_nodes if graph.degree(n) == 0]
    inr = len(isolated_explicit) / max(len(explicit_nodes), 1)
    results["isolated_node_rate"] = {
        "formula": "INR = |V_degree=0| / |V_total|",
        "value": inr,
        "isolated_count": len(isolated_explicit),
        "total_explicit": len(explicit_nodes),
        "status": "exact",
    }

    components = list(nx.weakly_connected_components(graph))
    largest = max((len(c) for c in components), default=0)
    lccr = largest / max(graph.number_of_nodes(), 1)
    results["largest_connected_component_ratio"] = {
        "formula": "LCCR = |V_LCC| / |V_total|",
        "value": lccr,
        "largest_component_size": largest,
        "total_graph_nodes": graph.number_of_nodes(),
        "status": "exact",
    }

    density = nx.density(graph)
    if expected_density is None:
        expected_density = 0.00003
        density_status = "proxy"
    else:
        density_status = "exact"
    deviation_abs = abs(density - expected_density)
    deviation_ratio = deviation_abs / max(expected_density, 1e-12)
    results["network_density_deviation"] = {
        "formula": "Density = |E| / (|V|(|V|-1)); Deviation=|D_ext-D_expected|",
        "density": density,
        "expected_density": expected_density,
        "deviation_abs": deviation_abs,
        "deviation_ratio": deviation_ratio,
        "status": density_status,
    }

    isolated_types = defaultdict(int)
    for eid in isolated_explicit:
        isolated_types[entities[eid].get("entity_type", "unknown")] += 1

    results["entity_summary"] = {
        "nodes": len(explicit_nodes),
        "edges": len(extracted_edge_set),
        "connected_nodes": len(explicit_nodes) - len(isolated_explicit),
        "isolated_nodes": len(isolated_explicit),
        "isolated_ratio": inr,
        "top_isolated_types": sorted(isolated_types.items(), key=lambda x: x[1], reverse=True)[:10],
    }
    return results


def compute_layout_metrics(entities, explicit_nodes, doc_meta, gt_layout_path=None, use_llm=False, llm_judge=None, alias_table=None):
    results = {}
    by_doc = defaultdict(list)
    gt_layout = load_layout_gt(gt_layout_path)
    for eid in explicit_nodes:
        ent = entities.get(eid)
        if ent:
            by_doc[ent.get("_doc_id", "")].append(ent)

    # 1) ROA
    inversions = 0
    seq_edges = 0
    roa_doc_scores = []

    # 2) NTED
    nted_scores = []

    # 3) 表格二维拓扑召回
    table_recalls = []
    exact_docs = 0

    for doc_id, doc_entities in by_doc.items():
        meta = doc_meta.get(doc_id, {})
        text = read_text_file(meta.get("source_path"))
        if not text:
            continue

        seq_entities = sorted(doc_entities, key=lambda e: (e.get("_source_line") is None, e.get("_source_line", 10**9)))
        names = [normalize_name(e.get("name") or e.get("id")) for e in seq_entities]
        positions = first_occurrence_order(text, names, alias_table=alias_table)

        doc_gt = gt_layout.get(doc_id)
        use_exact = doc_gt is not None

        if use_exact:
            exact_docs += 1
            gt_order = [normalize_name(x) for x in doc_gt.get("reading_order", []) if normalize_name(x)]
            pred_order = [normalize_name(x) for x in names if normalize_name(x)]

            gt_rank = {name: i for i, name in enumerate(gt_order)}
            pred_rank = {name: i for i, name in enumerate(pred_order)}
            common = [n for n in pred_order if n in gt_rank]
            local_edges = 0
            local_inversions = 0
            for i in range(len(common) - 1):
                a = common[i]
                b = common[i + 1]
                local_edges += 1
                if gt_rank[a] > gt_rank[b]:
                    local_inversions += 1
            if local_edges > 0:
                score = 1.0 - (local_inversions / local_edges)
                roa_doc_scores.append(score)
                inversions += local_inversions
                seq_edges += local_edges

            ext_tree = build_extracted_tree(text, seq_entities)
            gt_sections = doc_gt.get("sections", [])
            gt_tree = build_gt_tree_from_layout_sections(gt_sections) if gt_sections else build_gt_tree_from_text(text, seq_entities)
        else:
            local_edges = 0
            local_inversions = 0
            for idx in range(len(seq_entities) - 1):
                n1 = normalize_name(seq_entities[idx].get("name") or seq_entities[idx].get("id"))
                n2 = normalize_name(seq_entities[idx + 1].get("name") or seq_entities[idx + 1].get("id"))
                if not n1 or not n2:
                    continue
                if n1 not in positions or n2 not in positions:
                    continue
                local_edges += 1
                if positions[n1] > positions[n2]:
                    local_inversions += 1
            if local_edges > 0:
                score = 1.0 - (local_inversions / local_edges)
                roa_doc_scores.append(score)
                inversions += local_inversions
                seq_edges += local_edges

            ext_tree = build_extracted_tree(text, seq_entities)
            gt_tree = build_gt_tree_from_text(text, seq_entities)

        ted = ordered_tree_edit_distance(ext_tree, gt_tree)
        max_size = max(subtree_size(ext_tree), subtree_size(gt_tree), 1)
        nted = 1.0 - (ted / max_size)
        nted_scores.append(max(0.0, min(1.0, nted)))

        # 表格拓扑召回（启发式）
        if use_exact:
            gt_edges = set()
            for item in doc_gt.get("table_edges", []):
                if isinstance(item, dict):
                    h = normalize_table_header(item.get("header", ""))
                    c = normalize_table_cell(item.get("cell", ""))
                    if h and c:
                        gt_edges.add((h, c))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    h = normalize_table_header(item[0])
                    c = normalize_table_cell(item[1])
                    if h and c:
                        gt_edges.add((h, c))
        else:
            gt_edges = extract_table_topology_edges_from_text(text)
        ext_edges = extract_table_topology_edges_from_entities(seq_entities)
        if gt_edges:
            matched = len(gt_edges & ext_edges)
            recall = matched / len(gt_edges)
            table_recalls.append(recall)

    if seq_edges > 0:
        roa = 1.0 - (inversions / seq_edges)
        roa_status = "exact" if exact_docs > 0 else "proxy"
    else:
        roa = 0.0
        roa_status = "skipped"

    if nted_scores:
        nted_value = statistics.mean(nted_scores)
        nted_status = "exact" if exact_docs > 0 else "proxy"
    else:
        nted_value = 0.0
        nted_status = "skipped"

    if table_recalls:
        table_recall = statistics.mean(table_recalls)
        table_status = "exact" if exact_docs > 0 else "proxy"
    else:
        table_recall = 0.0
        table_status = "skipped"

    # 可选 LLM 校正：只让 LLM 产出结构化中间态，再由本地算法计算最终指标。
    if use_llm and llm_judge is not None:
        sampled_docs = random.sample(list(by_doc.keys()), min(5, len(by_doc)))
        llm_scores = {"roa": [], "nted": [], "table_topology_recall": []}
        for doc_id in sampled_docs:
            meta = doc_meta.get(doc_id, {})
            text = read_text_file(meta.get("source_path"))
            if not text:
                continue
            doc_entities = by_doc.get(doc_id, [])
            excerpt = text[:7000]
            payload = {
                "doc_id": doc_id,
                "entities": [
                    {
                        "name": e.get("name", ""),
                        "entity_type": e.get("entity_type", "unknown"),
                        "source_line": e.get("_source_line"),
                    }
                    for e in sorted(doc_entities, key=lambda x: (x.get("_source_line") is None, x.get("_source_line", 10**9)))[:120]
                ],
                "ocr_excerpt": excerpt,
            }
            messages = [
                {
                    "role": "system",
                    "content": (
                        "你是文档结构重建器。请基于OCR片段和实体列表输出结构化中间态JSON，"
                        "格式为{\"reading_order\":[...],\"sections\":[{\"title\":...,\"entities\":[...]}],"
                        "\"table_edges\":[{\"header\":...,\"cell\":...}] }。"
                        "禁止输出任何评分字段。"
                        "只返回JSON。"
                    ),
                },
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ]
            parsed = llm_judge.chat_json(messages)
            if not isinstance(parsed, dict):
                continue

            pred_order = [normalize_name(e.get("name") or e.get("id")) for e in doc_entities]
            pred_order = [x for x in pred_order if x]
            llm_order = [normalize_name(x) for x in parsed.get("reading_order", []) if normalize_name(x)]
            if llm_order and pred_order:
                gt_rank = {name: i for i, name in enumerate(llm_order)}
                common = [n for n in pred_order if n in gt_rank]
                local_edges = 0
                local_inv = 0
                for i in range(len(common) - 1):
                    a = common[i]
                    b = common[i + 1]
                    local_edges += 1
                    if gt_rank[a] > gt_rank[b]:
                        local_inv += 1
                if local_edges > 0:
                    llm_scores["roa"].append(1.0 - (local_inv / local_edges))

            ext_tree = build_extracted_tree(text, doc_entities)
            llm_sections = parsed.get("sections", [])
            if isinstance(llm_sections, list) and llm_sections:
                llm_tree = build_gt_tree_from_layout_sections(llm_sections)
                ted = ordered_tree_edit_distance(ext_tree, llm_tree)
                max_size = max(subtree_size(ext_tree), subtree_size(llm_tree), 1)
                llm_scores["nted"].append(max(0.0, min(1.0, 1.0 - (ted / max_size))))

            gt_edges = set()
            for item in parsed.get("table_edges", []) or []:
                if isinstance(item, dict):
                    h = normalize_table_header(item.get("header", ""))
                    c = normalize_table_cell(item.get("cell", ""))
                    if h and c:
                        gt_edges.add((h, c))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    h = normalize_table_header(item[0])
                    c = normalize_table_cell(item[1])
                    if h and c:
                        gt_edges.add((h, c))
            ext_edges = extract_table_topology_edges_from_entities(doc_entities)
            if gt_edges:
                llm_scores["table_topology_recall"].append(len(gt_edges & ext_edges) / len(gt_edges))

        # 融合：70% heuristic + 30% llm
        if llm_scores["roa"]:
            roa = 0.7 * roa + 0.3 * statistics.mean(llm_scores["roa"])
            roa_status = "llm_judged"
        if llm_scores["nted"]:
            nted_value = 0.7 * nted_value + 0.3 * statistics.mean(llm_scores["nted"])
            nted_status = "llm_judged"
        if llm_scores["table_topology_recall"]:
            table_recall = 0.7 * table_recall + 0.3 * statistics.mean(llm_scores["table_topology_recall"])
            table_status = "llm_judged"

    results["reading_order_accuracy"] = {
        "formula": "ROA = 1 - N_inversions / |E_seq|",
        "value": max(0.0, min(1.0, roa)),
        "inversions": inversions,
        "sequence_edges": seq_edges,
        "status": roa_status,
    }
    results["normalized_tree_edit_distance"] = {
        "formula": "NTED = 1 - TED(T_ext, T_gt) / max(|T_ext|, |T_gt|)",
        "value": max(0.0, min(1.0, nted_value)),
        "status": nted_status,
    }
    results["table_topology_recall"] = {
        "formula": "TableRecall = |E_table_ext ∩ E_table_gt| / |E_table_gt|",
        "value": max(0.0, min(1.0, table_recall)),
        "status": table_status,
    }
    return results


def extract_table_topology_edges_from_text(text):
    lines = text.splitlines()
    edges = set()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # 支持 | 分隔或多空格列
        if "|" in line:
            header = [normalize_name(x) for x in line.split("|") if normalize_name(x)]
            if len(header) >= 2:
                j = i + 1
                while j < len(lines):
                    row = lines[j].strip()
                    if not row or "|" not in row:
                        break
                    cells = [normalize_name(x) for x in row.split("|") if normalize_name(x)]
                    for h, c in zip(header, cells):
                        edges.add((normalize_table_header(h), normalize_table_cell(c)))
                    j += 1
                i = j
                continue

        cols = [c for c in re.split(r"\s{2,}", line) if c.strip()]
        if len(cols) >= 2:
            header = [normalize_name(x) for x in cols]
            j = i + 1
            row_count = 0
            while j < len(lines):
                row = lines[j].strip()
                if not row:
                    break
                row_cols = [c for c in re.split(r"\s{2,}", row) if c.strip()]
                if len(row_cols) < 2:
                    break
                for h, c in zip(header, row_cols):
                    edges.add((normalize_table_header(h), normalize_table_cell(c)))
                row_count += 1
                j += 1
            if row_count > 0:
                i = j
                continue
        i += 1
    return edges


def normalize_table_header(value):
    text = normalize_name(value).lower()
    alias = {
        "name": "name",
        "parameter": "name",
        "参数": "name",
        "type": "type",
        "类型": "type",
        "description": "description",
        "描述": "description",
        "remarks": "description",
        "说明": "description",
    }
    return alias.get(text, text)


def normalize_table_cell(value):
    text = normalize_name(value).lower()
    text = re.sub(r"\s+", " ", text)
    return text


def extract_table_topology_edges_from_entities(doc_entities):
    edges = set()
    for entity in doc_entities:
        for p in entity.get("parameters", []) or []:
            name = normalize_table_cell((p or {}).get("name", ""))
            ptype = normalize_table_cell((p or {}).get("type", ""))
            desc = normalize_table_cell((p or {}).get("description", ""))
            if name:
                edges.add(("name", name))
            if ptype:
                edges.add(("type", ptype))
            if desc:
                edges.add(("description", desc))
    return edges


def build_semantic_tokens(node_attrs):
    name = normalize_name(node_attrs.get("name", "")).lower()
    entity_type = normalize_name(node_attrs.get("entity_type", "unknown")).lower()
    tokens = set()
    tokens.update([x for x in re.split(r"[^a-zA-Z0-9\u4e00-\u9fff]+", name) if x])
    if name:
        tokens.add(name)
    if entity_type:
        tokens.add(entity_type)
    return tokens


def build_text_embedding(text, dim=128):
    vector = [0.0] * max(dim, 8)
    raw = normalize_name(text).lower()
    if not raw:
        return vector

    # token + char-gram 哈希嵌入，作为轻量级连续向量近似。
    items = [x for x in re.split(r"[^a-zA-Z0-9\u4e00-\u9fff]+", raw) if x]
    compact = re.sub(r"\s+", "", raw)
    for n in (2, 3, 4):
        if len(compact) >= n:
            for i in range(0, len(compact) - n + 1):
                items.append(compact[i:i + n])

    if not items:
        return vector

    for item in items:
        # 使用稳定哈希，避免 Python 进程随机种子导致的跨运行漂移。
        digest = hashlib.blake2b(item.encode("utf-8", errors="ignore"), digest_size=8).digest()
        h = int.from_bytes(digest, byteorder="big", signed=False)
        idx = h % len(vector)
        sign = -1.0 if ((h >> 1) & 1) else 1.0
        weight = 1.0 / max(len(item), 1)
        vector[idx] += sign * weight

    norm = math.sqrt(sum(v * v for v in vector))
    if norm > 1e-12:
        vector = [v / norm for v in vector]
    return vector


def cosine_similarity(v1, v2):
    if not v1 or not v2:
        return 0.0
    n = min(len(v1), len(v2))
    if n <= 0:
        return 0.0
    dot = 0.0
    n1 = 0.0
    n2 = 0.0
    for i in range(n):
        a = v1[i]
        b = v2[i]
        dot += a * b
        n1 += a * a
        n2 += b * b
    if n1 <= 1e-12 or n2 <= 1e-12:
        return 0.0
    return dot / math.sqrt(n1 * n2)


def build_semantic_index(graph, nodes, embedding_dim=128):
    index = {}
    for node in nodes:
        attrs = graph.nodes[node] if node in graph else {}
        name = normalize_name(attrs.get("name", "")).lower()
        etype = normalize_name(attrs.get("entity_type", "unknown")).lower()
        tokens = build_semantic_tokens(attrs)
        embedding_text = f"{name} [TYPE] {etype}".strip()
        index[node] = {
            "tokens": tokens,
            "entity_type": etype,
            "embedding": build_text_embedding(embedding_text, dim=embedding_dim),
        }
    return index


def semantic_pair_score(
    graph,
    u,
    v,
    semantic_index=None,
    similarity_mode="hybrid",
    lexical_weight=0.45,
    embedding_dim=128,
):
    semantic_index = semantic_index or {}
    info_u = semantic_index.get(u)
    info_v = semantic_index.get(v)

    if info_u is None:
        attrs_u = graph.nodes[u] if u in graph else {}
        info_u = {
            "tokens": build_semantic_tokens(attrs_u),
            "entity_type": normalize_name(attrs_u.get("entity_type", "unknown")).lower(),
            "embedding": build_text_embedding(
                f"{normalize_name(attrs_u.get('name', '')).lower()} [TYPE] {normalize_name(attrs_u.get('entity_type', 'unknown')).lower()}",
                dim=embedding_dim,
            ),
        }
        semantic_index[u] = info_u
    if info_v is None:
        attrs_v = graph.nodes[v] if v in graph else {}
        info_v = {
            "tokens": build_semantic_tokens(attrs_v),
            "entity_type": normalize_name(attrs_v.get("entity_type", "unknown")).lower(),
            "embedding": build_text_embedding(
                f"{normalize_name(attrs_v.get('name', '')).lower()} [TYPE] {normalize_name(attrs_v.get('entity_type', 'unknown')).lower()}",
                dim=embedding_dim,
            ),
        }
        semantic_index[v] = info_v

    tokens_u = info_u.get("tokens", set())
    tokens_v = info_v.get("tokens", set())
    if not tokens_u or not tokens_v:
        token_jaccard = 0.0
    else:
        token_jaccard = len(tokens_u & tokens_v) / max(len(tokens_u | tokens_v), 1)

    type_u = info_u.get("entity_type", "unknown")
    type_v = info_v.get("entity_type", "unknown")
    same_type_bonus = 0.2 if type_u and type_u == type_v and type_u != "unknown" else 0.0

    embedding_u = info_u.get("embedding", [])
    embedding_v = info_v.get("embedding", [])
    emb_cos = cosine_similarity(embedding_u, embedding_v)
    emb_score = max(0.0, min(1.0, 0.5 * (emb_cos + 1.0)))

    mode = normalize_name(similarity_mode).lower() or "hybrid"
    lw = min(max(safe_float(lexical_weight, 0.45), 0.0), 1.0)

    lexical_score = min(1.0, token_jaccard + same_type_bonus)
    if mode == "lexical":
        return lexical_score
    if mode == "embedding":
        return min(1.0, emb_score + 0.1 * (1.0 if same_type_bonus > 0 else 0.0))
    # hybrid
    return min(1.0, lw * lexical_score + (1.0 - lw) * emb_score)


def relation_transition_allowed(prev_rel, next_rel, transition_rules=None):
    transition_rules = transition_rules or {}
    if not prev_rel:
        return True
    default_set = transition_rules.get("*", {"cross_reference", "returns_type", "has_parameter", "related_to"})
    return next_rel in transition_rules.get(prev_rel, default_set)


def weighted_relation_choice(rng, edges, relation_weights):
    if not edges:
        return None
    filtered = []
    weights = []
    for edge in edges:
        _, rel = edge
        w = safe_float(relation_weights.get(rel, relation_weights.get("*", 1.0)), 1.0)
        if w <= 0:
            continue
        filtered.append(edge)
        weights.append(w)

    if not filtered:
        return None

    total = sum(weights)
    if total <= 0:
        return rng.choice(filtered)

    point = rng.random() * total
    acc = 0.0
    for edge, w in zip(filtered, weights):
        acc += w
        if acc >= point:
            return edge
    return filtered[-1]


def mine_hard_negative_pairs(
    graph,
    nodes,
    k,
    hard_target,
    semantic_threshold=0.15,
    semantic_index=None,
    similarity_mode="hybrid",
    lexical_weight=0.45,
    embedding_dim=128,
):
    if hard_target <= 0 or not nodes:
        return set(), {
            "strategy": "deterministic_ranked_mask",
            "examined_candidates": 0,
            "accepted_candidates": 0,
        }

    by_type = defaultdict(list)
    for n in nodes:
        n_type = normalize_name(graph.nodes[n].get("entity_type", "unknown")).lower() if n in graph else "unknown"
        by_type[n_type].append(n)

    reach_cache = {}

    def get_reachable(source):
        if source in reach_cache:
            return reach_cache[source]
        reachable = set(nx.single_source_shortest_path_length(graph, source, cutoff=max(k, 1)).keys())
        reach_cache[source] = reachable
        return reachable

    # 候选生成：限制源节点数量与候选池规模，随后排序并应用 k-hop 可达掩码。
    rng = random.Random(42)
    ranked_sources = sorted(nodes, key=lambda n: graph.out_degree(n), reverse=True)
    max_sources = min(len(ranked_sources), max(800, hard_target * 4))
    sources = ranked_sources[:max_sources]

    candidates = []
    examined = 0
    for source in sources:
        source_type = normalize_name(graph.nodes[source].get("entity_type", "unknown")).lower() if source in graph else "unknown"
        pool = set(list(graph.predecessors(source))[:40])
        same_type_nodes = by_type.get(source_type, [])
        if same_type_nodes:
            pick_size = min(40, len(same_type_nodes))
            pool.update(rng.sample(same_type_nodes, pick_size))

        if source in pool:
            pool.remove(source)
        if not pool:
            continue

        reachable = get_reachable(source)
        for target in pool:
            examined += 1
            if target in reachable:
                continue
            score = semantic_pair_score(
                graph,
                source,
                target,
                semantic_index=semantic_index,
                similarity_mode=similarity_mode,
                lexical_weight=lexical_weight,
                embedding_dim=embedding_dim,
            )
            if score < semantic_threshold:
                continue
            # 先保留高语义分，再以节点度做次级打散，减少极端集中。
            degree_bias = graph.degree(source) + graph.degree(target)
            candidates.append((score, -degree_bias, source, target))

    candidates.sort(key=lambda x: (-x[0], x[1]))

    hard_pairs = set()
    for _score, _bias, source, target in candidates:
        pair = (source, target)
        if pair in hard_pairs:
            continue
        hard_pairs.add(pair)
        if len(hard_pairs) >= hard_target:
            break

    return hard_pairs, {
        "strategy": "deterministic_ranked_mask",
        "examined_candidates": examined,
        "accepted_candidates": len(hard_pairs),
    }


def generate_query_pairs(
    graph,
    explicit_nodes,
    max_pairs=800,
    relation_weights=None,
    transition_rules=None,
    hard_negative_ratio=0.2,
    k=3,
    semantic_index=None,
    similarity_mode="hybrid",
    lexical_weight=0.45,
    embedding_dim=128,
):
    nodes = [n for n in explicit_nodes if n in graph]
    if not nodes:
        return {"pairs": [], "positive_pairs": 0, "hard_negative_pairs": 0}

    relation_weights = relation_weights or {"*": 1.0}
    transition_rules = transition_rules or {"*": {"cross_reference", "returns_type", "has_parameter", "related_to"}}
    hard_negative_ratio = min(max(hard_negative_ratio, 0.0), 0.9)

    rng = random.Random(42)
    positives = set()
    walk_seeds = [n for n in nodes if graph.out_degree(n) > 0]
    if not walk_seeds:
        walk_seeds = nodes

    hard_target = int(max_pairs * hard_negative_ratio)
    positive_target = max(max_pairs - hard_target, 0)

    def get_weighted_out_edges(node, prev_rel=""):
        out_edges = []
        for _, nxt, attrs in graph.out_edges(node, data=True):
            rel = normalize_name(attrs.get("relation", "related_to")).lower() or "related_to"
            if not relation_transition_allowed(prev_rel, rel, transition_rules):
                continue
            weight = safe_float(relation_weights.get(rel, relation_weights.get("*", 1.0)), 1.0)
            if weight <= 0:
                continue
            out_edges.append((nxt, rel))
        return out_edges

    max_walks = min(4000, len(nodes) * 3)
    for _ in range(max_walks):
        start = rng.choice(walk_seeds)
        if start not in graph:
            continue
        path_len = rng.randint(1, 4)
        current = start
        visited = {start}
        prev_rel = ""
        valid_steps = 0
        for _step in range(path_len):
            out_edges = [edge for edge in get_weighted_out_edges(current, prev_rel) if edge[0] not in visited]
            if not out_edges:
                break

            picked = weighted_relation_choice(rng, out_edges, relation_weights)
            if picked is None:
                break
            nxt, rel = picked
            visited.add(nxt)
            current = nxt
            prev_rel = rel
            valid_steps += 1
        if current != start and valid_steps > 0:
            positives.add((start, current))
        if len(positives) >= positive_target:
            break

    if len(positives) < positive_target:
        for source in nodes[: min(1000, len(nodes))]:
            one_hop = get_weighted_out_edges(source, "")
            for n, rel1 in one_hop:
                two_hop = get_weighted_out_edges(n, rel1)
                for target, _rel2 in two_hop:
                    if source == target:
                        continue
                    positives.add((source, target))
                if len(positives) >= positive_target:
                    break
            if len(positives) >= positive_target:
                break

    hard_pairs, hard_meta = mine_hard_negative_pairs(
        graph,
        nodes,
        k=k,
        hard_target=hard_target,
        semantic_threshold=0.15,
        semantic_index=semantic_index,
        similarity_mode=similarity_mode,
        lexical_weight=lexical_weight,
        embedding_dim=embedding_dim,
    )
    hard_pairs = {p for p in hard_pairs if p not in positives}
    if len(hard_pairs) > hard_target:
        hard_pairs = set(sorted(hard_pairs)[:hard_target])

    selected_positives = sorted(positives)
    if len(selected_positives) > positive_target:
        selected_positives = rng.sample(selected_positives, positive_target)

    pairs = list(selected_positives) + list(sorted(hard_pairs))
    if len(pairs) < max_pairs and len(positives) > len(selected_positives):
        leftovers = [p for p in sorted(positives) if p not in set(selected_positives)]
        needed = min(max_pairs - len(pairs), len(leftovers))
        if needed > 0:
            pairs.extend(leftovers[:needed])

    if len(pairs) > max_pairs:
        pairs = pairs[:max_pairs]

    return {
        "pairs": pairs,
        "positive_pairs": len(selected_positives),
        "hard_negative_pairs": len(hard_pairs),
        "hard_negative_target": hard_target,
        "hard_negative_meta": hard_meta,
    }


def k_hop_reachability(graph, query_pairs, k):
    if not query_pairs:
        return 0.0
    hits = 0
    grouped_targets = defaultdict(set)
    for source, target in query_pairs:
        grouped_targets[source].add(target)

    for source, targets in grouped_targets.items():
        if source not in graph:
            continue
        reach = nx.single_source_shortest_path_length(graph, source, cutoff=k)
        for target in targets:
            if target in reach and reach[target] <= k:
                hits += 1
    return hits / len(query_pairs)


def compute_fragility(graph, edge_confidence, query_pairs, k, removal_steps=None):
    if removal_steps is None:
        removal_steps = [0.1, 0.2, 0.3, 0.4, 0.5]
    if graph.number_of_edges() == 0 or not query_pairs:
        return {"vulnerability": 0.0, "slope": 0.0, "series": []}

    edges = list(graph.edges())
    # 按边置信度从低到高删除
    edges_sorted = sorted(
        edges,
        key=lambda e: edge_confidence.get((e[0], e[1], graph.edges[e].get("relation", "related_to")), 0.5),
    )

    original_score = k_hop_reachability(graph, query_pairs, k)
    xs = [0.0]
    ys = [original_score]
    series = [{"removed_ratio": 0.0, "k_hrr": original_score}]

    for ratio in removal_steps:
        remove_count = int(len(edges_sorted) * ratio)
        removed = set(edges_sorted[:remove_count])
        g2 = graph.copy()
        g2.remove_edges_from(list(removed))
        score = k_hop_reachability(g2, query_pairs, k)
        xs.append(ratio)
        ys.append(score)
        series.append({"removed_ratio": ratio, "k_hrr": score})

    # 线性回归斜率
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    denominator = sum((x - x_mean) ** 2 for x in xs)
    slope = numerator / denominator if denominator else 0.0
    vulnerability = max(0.0, -slope)
    return {
        "vulnerability": vulnerability,
        "slope": slope,
        "series": series,
    }


def predict_hidden_edges_density(
    graph,
    explicit_nodes,
    tau=0.35,
    max_candidates=15000,
    semantic_index=None,
    similarity_mode="hybrid",
    lexical_weight=0.45,
    embedding_dim=128,
):
    # MED_{>tau} 代理实现：融合多种局部链接预测分数，优于单一 Jaccard。
    undirected = nx.Graph()
    undirected.add_nodes_from(explicit_nodes)
    for source, target in graph.edges():
        if source in explicit_nodes and target in explicit_nodes:
            undirected.add_edge(source, target)

    nodes = list(explicit_nodes)
    candidates = set()
    sampled = 0
    for i in range(len(nodes)):
        if sampled >= max_candidates:
            break
        u = nodes[i]
        neighbors = set(undirected.neighbors(u))
        second_order = set()
        for n in neighbors:
            second_order.update(undirected.neighbors(n))
        second_order.discard(u)
        for v in second_order:
            if u == v or undirected.has_edge(u, v):
                continue
            pair = (u, v) if u <= v else (v, u)
            if pair in candidates:
                continue
            candidates.add(pair)
            sampled += 1
            if sampled >= max_candidates:
                break

    # 稀疏图补样：引入语义候选，降低仅靠局部邻域造成的盲区。
    if len(candidates) < max_candidates:
        grouped = defaultdict(list)
        for n in nodes:
            et = normalize_name(graph.nodes[n].get("entity_type", "unknown")).lower() if n in graph else "unknown"
            grouped[et].append(n)
        rng = random.Random(42)
        for _, bucket in grouped.items():
            if len(bucket) < 2:
                continue
            bucket_copy = list(bucket)
            rng.shuffle(bucket_copy)
            for i in range(min(len(bucket_copy), 120)):
                u = bucket_copy[i]
                for j in range(i + 1, min(len(bucket_copy), i + 12)):
                    v = bucket_copy[j]
                    if u == v or undirected.has_edge(u, v):
                        continue
                    pair = (u, v) if u <= v else (v, u)
                    if pair in candidates:
                        continue
                    if semantic_pair_score(
                        graph,
                        pair[0],
                        pair[1],
                        semantic_index=semantic_index,
                        similarity_mode=similarity_mode,
                        lexical_weight=lexical_weight,
                        embedding_dim=embedding_dim,
                    ) < 0.2:
                        continue
                    candidates.add(pair)
                    if len(candidates) >= max_candidates:
                        break
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= max_candidates:
                break

    candidates = list(candidates)

    if not candidates:
        return {
            "value": 0.0,
            "predicted_high_conf_edges": 0,
            "existing_edges": undirected.number_of_edges(),
            "tau": tau,
            "status": "proxy",
        }

    jc = {(u, v): s for u, v, s in nx.jaccard_coefficient(undirected, candidates)}
    aa = {(u, v): s for u, v, s in nx.adamic_adar_index(undirected, candidates)}
    ra = {(u, v): s for u, v, s in nx.resource_allocation_index(undirected, candidates)}
    pa = {(u, v): s for u, v, s in nx.preferential_attachment(undirected, candidates)}

    def normalize_scores(score_map):
        if not score_map:
            return {}
        values = list(score_map.values())
        mn = min(values)
        mx = max(values)
        if mx <= mn:
            return {k: 0.0 for k in score_map.keys()}
        return {k: (v - mn) / (mx - mn) for k, v in score_map.items()}

    jc_n = normalize_scores(jc)
    aa_n = normalize_scores(aa)
    ra_n = normalize_scores(ra)
    pa_n = normalize_scores(pa)

    high = 0
    for candidate in candidates:
        semantic = semantic_pair_score(
            graph,
            candidate[0],
            candidate[1],
            semantic_index=semantic_index,
            similarity_mode=similarity_mode,
            lexical_weight=lexical_weight,
            embedding_dim=embedding_dim,
        )
        score = (
            0.25 * jc_n.get(candidate, 0.0)
            + 0.22 * aa_n.get(candidate, 0.0)
            + 0.18 * ra_n.get(candidate, 0.0)
            + 0.05 * pa_n.get(candidate, 0.0)
            + 0.30 * semantic
        )
        if score > tau:
            high += 1

    existing_edges = max(undirected.number_of_edges(), 1)
    density = high / existing_edges
    return {
        "value": density,
        "predicted_high_conf_edges": high,
        "existing_edges": undirected.number_of_edges(),
        "tau": tau,
        "status": "proxy",
    }


def compute_semantic_metrics(
    graph,
    explicit_nodes,
    edge_confidence,
    k=3,
    tau=0.35,
    query_pairs_path=None,
    relation_weights=None,
    transition_rules=None,
    hard_negative_ratio=0.2,
    similarity_mode="hybrid",
    embedding_dim=128,
    lexical_weight=0.45,
):
    hard_negative_target = 0
    hard_negative_meta = {}
    semantic_index = build_semantic_index(graph, explicit_nodes, embedding_dim=embedding_dim)
    if query_pairs_path and os.path.exists(query_pairs_path):
        raw = load_json(query_pairs_path)
        query_pairs = parse_query_pairs(raw)
        positive_pairs = len(query_pairs)
        hard_negative_pairs = 0
    else:
        generated = generate_query_pairs(
            graph,
            explicit_nodes,
            relation_weights=relation_weights,
            transition_rules=transition_rules,
            hard_negative_ratio=hard_negative_ratio,
            k=k,
            semantic_index=semantic_index,
            similarity_mode=similarity_mode,
            lexical_weight=lexical_weight,
            embedding_dim=embedding_dim,
        )
        query_pairs = generated["pairs"]
        positive_pairs = generated["positive_pairs"]
        hard_negative_pairs = generated["hard_negative_pairs"]
        hard_negative_target = generated.get("hard_negative_target", 0)
        hard_negative_meta = generated.get("hard_negative_meta", {})

    realized_ratio = safe_float(hard_negative_pairs / max(len(query_pairs), 1), 0.0)
    ratio_gap = abs(realized_ratio - hard_negative_ratio) if not (query_pairs_path and os.path.exists(query_pairs_path)) else 0.0
    ratio_warning = ""
    if not (query_pairs_path and os.path.exists(query_pairs_path)) and ratio_gap > 0.05:
        ratio_warning = "hard_negative_ratio 与目标偏差超过 5%，可能因图结构稀疏或约束过强导致。"

    k_hrr = k_hop_reachability(graph, query_pairs, k)
    fragility = compute_fragility(graph, edge_confidence, query_pairs, k)
    med = predict_hidden_edges_density(
        graph,
        explicit_nodes,
        tau=tau,
        semantic_index=semantic_index,
        similarity_mode=similarity_mode,
        lexical_weight=lexical_weight,
        embedding_dim=embedding_dim,
    )

    return {
        "k_hop_reachability_rate": {
            "formula": "k-HRR = |{(u,v) in Q | PathLength(u,v)<=k}| / |Q|",
            "value": k_hrr,
            "k": k,
            "query_pairs": len(query_pairs),
            "positive_pairs": positive_pairs,
            "hard_negative_pairs": hard_negative_pairs,
            "hard_negative_target": hard_negative_target if not (query_pairs_path and os.path.exists(query_pairs_path)) else 0,
            "hard_negative_ratio_target": hard_negative_ratio if not (query_pairs_path and os.path.exists(query_pairs_path)) else 0.0,
            "hard_negative_ratio": realized_ratio,
            "hard_negative_ratio_gap": ratio_gap,
            "hard_negative_ratio_warning": ratio_warning,
            "hard_negative_generation": hard_negative_meta,
            "semantic_similarity_mode": similarity_mode,
            "semantic_embedding_dim": embedding_dim,
            "semantic_lexical_weight": lexical_weight,
            "query_generation": "ground_truth" if query_pairs_path and os.path.exists(query_pairs_path) else "directed_meta_path_walk",
            "status": "exact" if query_pairs_path and os.path.exists(query_pairs_path) else "proxy",
        },
        "reasoning_chain_fragility": {
            "formula": "Fragility = -slope( k-HRR vs removed_edge_ratio )",
            "value": fragility["vulnerability"],
            "slope": fragility["slope"],
            "series": fragility["series"],
            "status": "proxy",
        },
        "high_confidence_implicit_edge_density": {
            "formula": "MED_{>tau} = |{e_pred | confidence>tau}| / |E_exist|",
            "value": med["value"],
            "predicted_high_conf_edges": med["predicted_high_conf_edges"],
            "existing_edges": med["existing_edges"],
            "tau": med["tau"],
            "status": med["status"],
        },
    }


def print_summary(report):
    topo = report["metrics"]["topology"]
    layout = report["metrics"]["layout"]
    semantic = report["metrics"]["semantic"]
    summary = topo["entity_summary"]

    print("=" * 72)
    print("知识图谱评估报告（基础拓扑 + 视觉结构 + 语义推理）")
    print("=" * 72)
    print("\n[整体图谱数据统计]")
    print(f"实体节点总数 (Nodes): {summary['nodes']} 个")
    print(f"直接边/关系总数 (Edges): {summary['edges']} 条（包含 cross_references 与出入参类型联系）")
    print(f"连通的实体节点: {summary['connected_nodes']} 个")

    print("\n还有孤立节点吗？")
    if summary["isolated_nodes"] > 0:
        print("有，而且占有一定比例。")
    else:
        print("没有，当前图没有孤立实体节点。")
    print(f"\n孤立节点数: {summary['isolated_nodes']} 个")
    print(f"孤立节点占比: 约 {summary['isolated_ratio']:.2%}")

    print("\n排名前列的孤立节点类型 (Top Isolated Entity Types):")
    for etype, count in summary["top_isolated_types"][:7]:
        print(f"{etype}: {count} 个")

    print("\n[一、基础拓扑结构定量指标]")
    print(f"节点召回率 Recall_node: {topo['node_recall']['value']:.4f} ({topo['node_recall']['status']})")
    edge_precision_value = topo["edge_precision"]["value"]
    if edge_precision_value is None:
        print(f"边精确率 Precision_edge: N/A ({topo['edge_precision']['status']})")
    else:
        print(f"边精确率 Precision_edge: {edge_precision_value:.4f} ({topo['edge_precision']['status']})")
    print(f"边信息密度 EdgeInfoDensity: {topo['edge_information_density']['value']:.4f}")
    print(f"孤立节点率 INR: {topo['isolated_node_rate']['value']:.4f}")
    print(f"最大连通分量占比 LCCR: {topo['largest_connected_component_ratio']['value']:.4f}")
    print(
        "网络密度偏离度 Deviation: "
        f"{topo['network_density_deviation']['deviation_abs']:.8f} "
        f"(density={topo['network_density_deviation']['density']:.8f})"
    )

    print("\n[二、视觉版式与物理结构定量指标]")
    print(f"阅读顺序准确率 ROA: {layout['reading_order_accuracy']['value']:.4f} ({layout['reading_order_accuracy']['status']})")
    print(
        "归一化树编辑距离 NTED: "
        f"{layout['normalized_tree_edit_distance']['value']:.4f} "
        f"({layout['normalized_tree_edit_distance']['status']})"
    )
    print(
        "表格二维拓扑召回率: "
        f"{layout['table_topology_recall']['value']:.4f} "
        f"({layout['table_topology_recall']['status']})"
    )

    print("\n[三、语义连贯性与推理定量指标]")
    print(f"k跳路径可达率 k-HRR: {semantic['k_hop_reachability_rate']['value']:.4f}")
    print(f"推理断链脆弱度: {semantic['reasoning_chain_fragility']['value']:.4f}")
    print(f"高置信度隐性边密度 MED_>tau: {semantic['high_confidence_implicit_edge_density']['value']:.4f}")
    print("=" * 72)


def save_report(report, output_path):
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)


def parse_args():
    parser = argparse.ArgumentParser(description="Knowledge Graph Comprehensive Metrics Evaluator")
    parser.add_argument("--json-pattern", default="json_output_v4/_p_*.json", help="Input json glob pattern")
    parser.add_argument("--workspace-dir", default=".", help="Workspace root directory")
    parser.add_argument("--gt-layout", default="", help="Optional ground-truth layout json")
    parser.add_argument("--gt-nodes", default="", help="Optional ground-truth nodes json")
    parser.add_argument("--gt-edges", default="", help="Optional ground-truth edges json")
    parser.add_argument("--expected-density", type=float, default=None, help="Expected density for deviation")
    parser.add_argument("--query-pairs", default="", help="Optional semantic query pairs json")
    parser.add_argument("--k", type=int, default=3, help="k for k-hop reachability")
    parser.add_argument("--tau", type=float, default=0.35, help="tau for MED_{>tau}")
    parser.add_argument("--use-llm", action="store_true", help="Enable optional LLM judge for layout metrics")
    parser.add_argument("--llm-config", default="llm_config_example.json", help="LLM config path")
    parser.add_argument("--alias-map", default="", help="Optional entity alias table json for CJK alignment")
    parser.add_argument("--meta-path-config", default="meta_path_templates.json", help="Meta-path templates config json")
    parser.add_argument("--meta-path-profile", default="default", help="Meta-path profile key")
    parser.add_argument("--hard-negative-ratio", type=float, default=0.2, help="Ratio of hard negative pairs in generated queries")
    parser.add_argument(
        "--semantic-sim-mode",
        choices=["lexical", "embedding", "hybrid"],
        default="hybrid",
        help="Semantic similarity mode used by hard negatives and MED",
    )
    parser.add_argument("--embedding-dim", type=int, default=128, help="Embedding dimension for local hashed embedding")
    parser.add_argument("--semantic-lexical-weight", type=float, default=0.45, help="Lexical weight in hybrid semantic mode")
    parser.add_argument("--output", default="json_output_v4/_kg_quality_eval_report.json", help="Output report json")
    parser.add_argument("--init-gt-templates", action="store_true", help="Create GT adapter templates and exit")
    parser.add_argument("--gt-template-dir", default="gt_templates", help="Directory for GT adapter templates")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.init_gt_templates:
        init_gt_templates(args.gt_template_dir)
        print(f"GT 模板已生成: {args.gt_template_dir}")
        return

    workspace_dir = os.path.abspath(args.workspace_dir)
    json_pattern = args.json_pattern
    alias_table = load_alias_table(args.alias_map)
    meta_policy = load_meta_path_policy(args.meta_path_config, args.meta_path_profile)
    if meta_policy["requested_profile"] != meta_policy["profile"]:
        options = ", ".join(meta_policy["available_profiles"])
        raise ValueError(f"无效的 --meta-path-profile: {meta_policy['requested_profile']}，可选值: {options}")

    effective_hard_negative_ratio = min(max(args.hard_negative_ratio, 0.0), 0.9)
    if abs(effective_hard_negative_ratio - args.hard_negative_ratio) > 1e-12:
        print(
            "[WARN] hard-negative-ratio 超出范围，已自动截断到 "
            f"{effective_hard_negative_ratio:.2f}"
        )

    print(f"正在读取数据: {json_pattern}")
    entities, explicit_nodes, edges, edge_confidence, doc_meta, _ = parse_entities_and_edges(
        json_pattern,
        workspace_dir,
    )
    graph = build_graph(explicit_nodes, edges, entities)

    llm_judge = None
    if args.use_llm:
        if os.path.exists(args.llm_config):
            llm_judge = LLMJudge(args.llm_config)
        else:
            print(f"[WARN] LLM 配置不存在，跳过 LLM 评估: {args.llm_config}")

    topology_metrics = compute_topology_metrics(
        graph,
        entities,
        explicit_nodes,
        edges,
        doc_meta,
        gt_nodes_path=args.gt_nodes if args.gt_nodes else None,
        gt_edges_path=args.gt_edges if args.gt_edges else None,
        expected_density=args.expected_density,
    )

    layout_metrics = compute_layout_metrics(
        entities,
        explicit_nodes,
        doc_meta,
        gt_layout_path=args.gt_layout if args.gt_layout else None,
        use_llm=bool(args.use_llm and llm_judge is not None),
        llm_judge=llm_judge,
        alias_table=alias_table,
    )

    semantic_metrics = compute_semantic_metrics(
        graph,
        explicit_nodes,
        edge_confidence,
        k=args.k,
        tau=args.tau,
        query_pairs_path=args.query_pairs if args.query_pairs else None,
        relation_weights=meta_policy["relation_weights"],
        transition_rules=meta_policy["transitions"],
        hard_negative_ratio=effective_hard_negative_ratio,
        similarity_mode=args.semantic_sim_mode,
        embedding_dim=max(int(args.embedding_dim), 8),
        lexical_weight=min(max(safe_float(args.semantic_lexical_weight, 0.45), 0.0), 1.0),
    )

    report = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "input": {
            "json_pattern": json_pattern,
            "workspace_dir": workspace_dir,
            "gt_layout": args.gt_layout,
            "gt_nodes": args.gt_nodes,
            "gt_edges": args.gt_edges,
            "query_pairs": args.query_pairs,
            "use_llm": bool(args.use_llm),
            "llm_config": args.llm_config,
            "alias_map": args.alias_map,
            "meta_path_config": args.meta_path_config,
            "meta_path_requested_profile": args.meta_path_profile,
            "meta_path_profile": meta_policy["profile"],
            "hard_negative_ratio_requested": args.hard_negative_ratio,
            "hard_negative_ratio": effective_hard_negative_ratio,
            "semantic_sim_mode": args.semantic_sim_mode,
            "embedding_dim": max(int(args.embedding_dim), 8),
            "semantic_lexical_weight": min(max(safe_float(args.semantic_lexical_weight, 0.45), 0.0), 1.0),
        },
        "metrics": {
            "topology": topology_metrics,
            "layout": layout_metrics,
            "semantic": semantic_metrics,
        },
    }

    print_summary(report)
    save_report(report, args.output)
    print(f"\n评估报告已写入: {args.output}")


if __name__ == "__main__":
    main()
