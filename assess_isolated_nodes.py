#!/usr/bin/env python3
"""
KG 孤立节点快速评估 - 轻量版

快速检查孤立节点改进效果，无需完整的语义/布局评估
"""

import json
import os
import re
import glob
from collections import defaultdict
import networkx as nx

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_output_v4")

def normalize_name(value):
    if value is None:
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_entities_and_edges_fast():
    """快速解析实体和边"""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "_p_*.json"))
    )
    
    entities = {}
    explicit_nodes = set()
    name_to_id = {}
    edges = set()
    
    print(f"处理 {len(files)} 个JSON文件...")
    
    # 第一轮：解析实体，建立名称到ID的映射
    for fp in files:
        try:
            data = load_json(fp)
        except:
            continue
        
        for entity in data.get("entities", []):
            eid = normalize_name(entity.get("id"))
            if not eid:
                continue
            
            if eid in entities:
                continue
            
            name = normalize_name(entity.get("name"))
            etype = normalize_name(entity.get("entity_type", "unknown")).lower() or "unknown"
            
            entities[eid] = {
                "name": name,
                "entity_type": etype
            }
            explicit_nodes.add(eid)
            
            # 建立名称到ID的映射
            if name:
                name_to_id[name] = eid
    
    # 第二轮：解析原始JSON中的关系
    for fp in files:
        try:
            data = load_json(fp)
        except:
            continue
        
        for entity in data.get("entities", []):
            sid = normalize_name(entity.get("id"))
            if not sid or sid not in explicit_nodes:
                continue
            
            # cross_references
            for ref in entity.get("cross_references", []) or []:
                tid = normalize_name(ref)
                if tid:
                    edges.add((sid, tid, "cross_reference"))
            
            # parameters  
            for p in entity.get("parameters", []) or []:
                ptype = normalize_name((p or {}).get("type"))
                if ptype:
                    edges.add((sid, ptype, "has_parameter"))
            
            # return_value
            rv = entity.get("return_value", {})
            ret_type = None
            if isinstance(rv, dict):
                ret_type = normalize_name(rv.get("type"))
            elif isinstance(rv, str):
                ret_type = normalize_name(rv)
            
            if ret_type and ret_type.lower() != "void":
                edges.add((sid, ret_type, "returns_type"))
        
        # relationships
        for rel in data.get("relationships", []):
            source = normalize_name(rel.get("source"))
            target = normalize_name(rel.get("target"))
            rtype = normalize_name(rel.get("type", "related_to")).lower()
            if source and target:
                edges.add((source, target, rtype))
    
    # 第三轮：加载 global_edges.json（已用名称表示）
    global_edges_path = os.path.join(OUT_DIR, "global_edges.json")
    merged = 0
    if os.path.exists(global_edges_path):
        try:
            global_data = load_json(global_edges_path)
            for edge_item in global_data.get("edges", []):
                source = normalize_name(edge_item.get("source"))
                target = normalize_name(edge_item.get("target"))
                etype = normalize_name(edge_item.get("type", "related_to")).lower()
                
                if source and target:
                    # 转换为ID（如果存在）或保持名称
                    source_id = name_to_id.get(source, source)
                    target_id = name_to_id.get(target, target)
                    
                    if (source_id, target_id, etype) not in edges:
                        edges.add((source_id, target_id, etype))
                        merged += 1
            
            print(f"[INFO] 合并 global_edges.json: 新增 {merged} 条边")
        except Exception as e:
            print(f"[WARN] 合并失败: {e}")
    
    print(f"[INFO] 名称到ID映射: {len(name_to_id)} 个")
    
    return entities, explicit_nodes, edges, name_to_id


def compute_isolated_nodes():
    """计算孤立节点统计"""
    entities, explicit_nodes, edges, name_to_id = parse_entities_and_edges_fast()
    
    print(f"\n实体总数: {len(entities)}")
    print(f"显式节点: {len(explicit_nodes)}")
    print(f"边总数: {len(edges)}")
    
    # 快速方式：直接从边构建邻接关系
    neighbors = defaultdict(set)
    connected_count = 0
    
    for source, target, relation in edges:
        # 检查是否涉及显式节点
        if source in explicit_nodes:
            neighbors[source].add(target)
            connected_count += 1
        if target in explicit_nodes:
            neighbors[target].add(source)
    
    # 计算孤立节点
    isolated = []
    for node in explicit_nodes:
        if len(neighbors[node]) == 0:
            isolated.append(node)
    
    print(f"\n有效关联节点数: {connected_count}")
    print(f"孤立节点数: {len(isolated)}")
    print(f"孤立节点占比: {len(isolated) / len(explicit_nodes):.2%}")
    print(f"连通节点占比: {(len(explicit_nodes) - len(isolated)) / len(explicit_nodes):.2%}")
    
    # 按类型统计
    isolated_by_type = defaultdict(int)
    for node in isolated:
        et = entities[node]["entity_type"]
        isolated_by_type[et] += 1
    
    print("\n孤立节点类型分布（Top 10）:")
    for et, count in sorted(isolated_by_type.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {et}: {count} 个({count / len(explicit_nodes) * 100:.1f}%)")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    compute_isolated_nodes()
