#!/usr/bin/env python3
"""
KG v4.2: 孤立节点补救 - 基于启发式规则连接孤立节点

四大策略：
1. Struct Members - struct 类型与其字段/成员连接
2. Enum Values - enum 类型与 enum_value 连接
3. Type Normalization - 改进参数/返回值类型匹配
4. Constants - constant 与相关 struct/enum 连接

使用方法：
  python kg_connect_isolated_v42.py --dry-run         # 预览改动
  python kg_connect_isolated_v42.py --apply           # 实际应用
"""

import json
import os
import re
import glob
import argparse
from collections import defaultdict
from copy import deepcopy

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")

C_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
POINTER_TRIM_RE = re.compile(r"[\s\*]+")

TYPE_NODE_KINDS = {
    "structure", "struct", "enum", "enum_value", "union",
    "typedef", "constant", "macro", "flags", "error_code",
}

C_KEYWORDS = {
    "const", "volatile", "signed", "unsigned", "struct", "enum",
    "union", "class", "typedef", "static", "extern", "inline",
    "__in", "__out", "__inout", "_In_", "_Out_", "_Inout_",
}


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data, dry_run=False):
    if dry_run:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def build_global_index():
    """构建 name -> {id, type, file} 映射"""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )
    name_to_info = defaultdict(list)
    entities = {}

    for fp in files:
        try:
            doc = load_json(fp)
            source_file = os.path.basename(fp)
            
            for ent in doc.get("entities", []):
                eid = ent.get("id")
                name = ent.get("name")
                if not eid or not name:
                    continue
                
                et = str(ent.get("entity_type", "unknown")).strip().lower()
                info = {
                    "id": eid,
                    "name": name,
                    "entity_type": et,
                    "source_file": source_file,
                    "entity": ent
                }
                name_to_info[name].append(info)
                entities[eid] = info
        except Exception as e:
            print(f"Warning: 读取 {fp} 失败: {e}")

    return entities, name_to_info


def tokenize_type_string(ts: str):
    """从类型字符串抽取候选类型名"""
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


def build_existing_edges(edges):
    """构建现有边的集合（双向）"""
    s = set()
    for e in edges:
        src = e.get("source") or e.get("from")
        tgt = e.get("target") or e.get("to")
        et = e.get("type") or e.get("edge_type", "unknown")
        if src and tgt:
            s.add((src, tgt, et))
            s.add((tgt, src, et))
    return s


# ============================================================================
# 策略 1: Struct Members - 结构体字段连接
# ============================================================================
def connect_struct_members(name_to_info, edges, existing_edges, dry_run=False):
    """将 struct 与其 members 连接"""
    new_edges = []
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    for fp in files:
        doc = load_json(fp)
        source_file = os.path.basename(fp)
        
        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "unknown")).strip().lower()
            if et not in ("structure", "struct"):
                continue
            
            struct_name = ent.get("name")
            if not struct_name:
                continue
            
            # 处理 members 字段
            for member in ent.get("members") or []:
                member_name = member.get("name")
                member_type = member.get("type")
                
                if member_name and member_type:
                    # 策略: struct_field 关系
                    for token in tokenize_type_string(member_type):
                        for target_info in name_to_info.get(token, []):
                            if target_info["entity_type"] in TYPE_NODE_KINDS:
                                edge_key = (struct_name, target_info["name"], "struct_field")
                                if edge_key not in existing_edges:
                                    edges.append({
                                        "source": struct_name,
                                        "target": target_info["name"],
                                        "type": "struct_field",
                                        "source_file": source_file,
                                        "detail": f"member: {member_name}",
                                        "_v42_strategy": "struct_members"
                                    })
                                    existing_edges.add(edge_key)
                                    existing_edges.add((target_info["name"], struct_name, "struct_field"))
                                    new_edges.append(edges[-1])
    
    return new_edges


# ============================================================================
# 策略 2: Enum Values - 枚举与成员连接
# ============================================================================
def connect_enum_values(name_to_info, edges, existing_edges, dry_run=False):
    """将 enum 与其 enum_value 连接"""
    new_edges = []
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    for fp in files:
        doc = load_json(fp)
        source_file = os.path.basename(fp)
        
        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "unknown")).strip().lower()
            if et != "enum":
                continue
            
            enum_name = ent.get("name")
            if not enum_name:
                continue
            
            # 处理 values 或 members 字段
            for value in ent.get("values") or []:
                if isinstance(value, dict):
                    val_name = value.get("name")
                else:
                    val_name = str(value)
                
                if val_name:
                    # 查找枚举值是否已有实体
                    for target_info in name_to_info.get(val_name, []):
                        if target_info["entity_type"] in ("enum_value", "constant"):
                            edge_key = (enum_name, target_info["name"], "enum_member")
                            if edge_key not in existing_edges:
                                edges.append({
                                    "source": enum_name,
                                    "target": target_info["name"],
                                    "type": "enum_member",
                                    "source_file": source_file,
                                    "_v42_strategy": "enum_values"
                                })
                                existing_edges.add(edge_key)
                                existing_edges.add((target_info["name"], enum_name, "enum_member"))
                                new_edges.append(edges[-1])
    
    return new_edges


# ============================================================================
# 策略 3: Type Normalization - 改进函数参数类型匹配
# ============================================================================
def improve_function_types(name_to_info, edges, existing_edges, dry_run=False):
    """增强函数参数和返回值的类型匹配"""
    new_edges = []
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    for fp in files:
        doc = load_json(fp)
        source_file = os.path.basename(fp)
        
        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "unknown")).strip().lower()
            if et != "function":
                continue
            
            func_name = ent.get("name")
            if not func_name:
                continue
            
            # 参数类型
            for param in ent.get("parameters") or []:
                param_type = param.get("type")
                if not param_type:
                    continue
                
                for token in tokenize_type_string(param_type):
                    # 模糊匹配：token 作为前缀或平凡匹配
                    candidates = []
                    
                    # 精确匹配
                    if token in name_to_info:
                        candidates.extend(name_to_info[token])
                    
                    # 前缀匹配（e.g., "HANDLE" 匹配 "HANDLE_*"）
                    for key in name_to_info:
                        if key.startswith(token) and len(key) < len(token) + 8:
                            candidates.extend(name_to_info[key])
                    
                    for target_info in candidates:
                        if target_info["entity_type"] in TYPE_NODE_KINDS:
                            edge_key = (func_name, target_info["name"], "uses_type")
                            if edge_key not in existing_edges:
                                edges.append({
                                    "source": func_name,
                                    "target": target_info["name"],
                                    "type": "uses_type",
                                    "source_file": source_file,
                                    "detail": f"param: {param_type}",
                                    "_v42_strategy": "function_params_enhanced"
                                })
                                existing_edges.add(edge_key)
                                existing_edges.add((target_info["name"], func_name, "uses_type"))
                                new_edges.append(edges[-1])
            
            # 返回值类型
            ret_val = ent.get("return_value")
            ret_type = None
            
            if isinstance(ret_val, dict):
                ret_type = ret_val.get("type")
            elif isinstance(ret_val, str):
                ret_type = ret_val
            
            if ret_type:
                for token in tokenize_type_string(ret_type):
                    candidates = []
                    
                    if token in name_to_info:
                        candidates.extend(name_to_info[token])
                    
                    for key in name_to_info:
                        if key.startswith(token) and len(key) < len(token) + 8:
                            candidates.extend(name_to_info[key])
                    
                    for target_info in candidates:
                        if target_info["entity_type"] in TYPE_NODE_KINDS:
                            edge_key = (func_name, target_info["name"], "returns_type")
                            if edge_key not in existing_edges:
                                edges.append({
                                    "source": func_name,
                                    "target": target_info["name"],
                                    "type": "returns_type",
                                    "source_file": source_file,
                                    "detail": f"return: {ret_type}",
                                    "_v42_strategy": "function_returns_enhanced"
                                })
                                existing_edges.add(edge_key)
                                existing_edges.add((target_info["name"], func_name, "returns_type"))
                                new_edges.append(edges[-1])
    
    return new_edges


# ============================================================================
# 策略 4: Constant Contextualization - 常量与相关类型连接
# ============================================================================
def connect_constants(name_to_info, edges, existing_edges, dry_run=False):
    """将常量与包含其名称前缀的 struct/enum 连接"""
    new_edges = []
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )

    for fp in files:
        doc = load_json(fp)
        source_file = os.path.basename(fp)
        
        for ent in doc.get("entities", []):
            et = str(ent.get("entity_type", "unknown")).strip().lower()
            if et != "constant":
                continue
            
            const_name = ent.get("name")
            if not const_name or len(const_name) < 4:
                continue
            
            # 尝试通过命名前缀连接
            # 例如: DXGI_FORMAT_R32G32B32_FLOAT -> DXGI_FORMAT
            parts = const_name.split("_")
            
            # 策略1: 前缀匹配（取前2-3个词）
            for prefix_len in [3, 2]:
                if len(parts) <= prefix_len:
                    continue
                
                prefix = "_".join(parts[:prefix_len])
                
                for name in name_to_info:
                    if name.startswith(prefix) and name in name_to_info:
                        for target_info in name_to_info[name]:
                            if target_info["entity_type"] in ("struct", "structure", "enum"):
                                edge_key = (const_name, target_info["name"], "belongs_to")
                                if edge_key not in existing_edges:
                                    edges.append({
                                        "source": const_name,
                                        "target": target_info["name"],
                                        "type": "belongs_to",
                                        "source_file": source_file,
                                        "_v42_strategy": "constant_contextualization"
                                    })
                                    existing_edges.add(edge_key)
                                    existing_edges.add((target_info["name"], const_name, "belongs_to"))
                                    new_edges.append(edges[-1])
    
    return new_edges


# ============================================================================
# 主函数
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="KG v4.2: 孤立节点补救")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不修改")
    parser.add_argument("--apply", action="store_true", help="实际应用修改")
    args = parser.parse_args()
    
    dry_run = not args.apply
    
    if not os.path.isdir(OUT_DIR):
        raise SystemExit(f"json_output_v4 目录不存在: {OUT_DIR}")
    
    print("=" * 70)
    print("[v4.2] 孤立节点补救 - 启发式规则连接")
    print("=" * 70)
    
    print("\n[v4.2] 构建全局索引 ...")
    entities, name_to_info = build_global_index()
    print(f"[v4.2] 实体总数: {len(entities)}, 唯一实体名: {len(name_to_info)}")
    
    # 加载现有边
    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    if os.path.exists(edges_path):
        edoc = load_json(edges_path)
        edges = edoc.get("edges") or edoc.get("data", {}).get("edges", [])
    else:
        edges = []
    
    print(f"[v4.2] 现有边数: {len(edges)}")
    existing_edges = build_existing_edges(edges)
    
    # 四大策略
    strategies = [
        ("Struct Members", connect_struct_members),
        ("Enum Values", connect_enum_values),
        ("Function Type Matching (Enhanced)", improve_function_types),
        ("Constant Contextualization", connect_constants),
    ]
    
    total_new = 0
    for strategy_name, strategy_func in strategies:
        print(f"\n[v4.2] 策略: {strategy_name} ...")
        new_edges = strategy_func(name_to_info, edges, existing_edges, dry_run)
        print(f"       新增边数: {len(new_edges)}")
        total_new += len(new_edges)
    
    print(f"\n[v4.2] 总新增边数: {total_new}")
    print(f"[v4.2] 新总边数: {len(edges)}")
    
    if dry_run:
        print("\n[v4.2] 干运行模式（--dry-run）- 未实际修改文件")
        print("       使用 --apply 标志进行实际应用")
    else:
        out_edges_path = os.path.join(OUT_DIR, "global_edges_v42.json")
        save_json(out_edges_path, {"edges": edges}, dry_run=False)
        print(f"\n[v4.2] 已写入: {out_edges_path}")
        
        # 备份原文件
        import shutil
        bak_path = edges_path + ".bak_v41"
        if not os.path.exists(bak_path):
            shutil.copy(edges_path, bak_path)
            print(f"[v4.2] 已备份原文件: {bak_path}")
        
        # 覆盖原文件
        shutil.copy(out_edges_path, edges_path)
        print(f"[v4.2] 已更新: {edges_path}")
    
    print("\n[v4.2] 完成！")
    print("      下一步: python evaluate_graph_metrics.py")


if __name__ == "__main__":
    main()
