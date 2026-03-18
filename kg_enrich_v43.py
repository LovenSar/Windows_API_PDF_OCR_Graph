#!/usr/bin/env python3
"""
KG v4.3: 孤立节点补救增强 - 五大高价值改进

包含:
1. Enum值实体动态生成 - 从enum定义创建enum_value
2. 类型别名规范化 - 处理LPVOID/HANDLE等Windows类型
3. Callback/Interface启发式规则 - 专门处理回调和接口
4. 参数类型深度解析 - typedef、指针等复杂类型
5. 质量评估 - 新增边的准确度采样检查

使用方法:
  python kg_enrich_v43.py --strategy all     # 应用所有策略
  python kg_enrich_v43.py --strategy enum-values  # 仅enum值
  python kg_enrich_v43.py --quality-check --sample 200  # 质量检查
"""

import json
import os
import re
import glob
import argparse
import random
from collections import defaultdict
from copy import deepcopy

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")

# ============================================================================
# 第1部分: Enum 值实体动态生成
# ============================================================================

class EnumValueGenerator:
    """从enum定义中动态生成enum_value实体"""
    
    @staticmethod
    def generate_from_enums(files_list, entities, edges, dry_run=False):
        """从现有enum生成enum_value实体和边"""
        new_entities = []
        new_edges = []
        generated_count = 0
        
        for fp in files_list:
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except:
                continue
            
            source_file = os.path.basename(fp)
            
            for ent in data.get("entities", []):
                etype = str(ent.get("entity_type", "")).strip().lower()
                if etype != "enum":
                    continue
                
                enum_name = ent.get("name", "").strip()
                if not enum_name:
                    continue
                
                # 处理 values/members 字段
                values = ent.get("values") or ent.get("members") or []
                
                for idx, val in enumerate(values):
                    if isinstance(val, dict):
                        val_name = val.get("name", "").strip()
                        val_value = val.get("value")
                    else:
                        val_name = str(val).strip()
                        val_value = idx
                    
                    if not val_name:
                        continue
                    
                    # 检查是否已存在
                    if val_name in entities:
                        continue
                    
                    # 创建enum_value实体
                    new_id = f"_enum_value_{enum_name}_{val_name}"
                    
                    new_entity = {
                        "id": new_id,
                        "name": val_name,
                        "entity_type": "enum_value",
                        "confidence": 0.95,
                        "_source_file": source_file,
                        "_parent_enum": enum_name,
                        "_generated_by": "v43_enum_generator"
                    }
                    
                    if val_value is not None:
                        new_entity["value"] = val_value
                    
                    new_entities.append(new_entity)
                    entities[new_id] = {"name": val_name, "entity_type": "enum_value"}
                    
                    # 创建enum -> enum_value边
                    edge = {
                        "source": enum_name,
                        "target": val_name,
                        "type": "enum_value",
                        "source_file": source_file,
                        "_v43_strategy": "enum_value_generation"
                    }
                    new_edges.append(edge)
                    generated_count += 1
        
        return new_entities, new_edges, generated_count


# ============================================================================
# 第2部分: 类型别名规范化
# ============================================================================

class TypeAliasNormalizer:
    """处理Windows API常见类型别名"""
    
    # 常见的Windows类型别名映射
    WIN32_ALIASES = {
        "LPVOID": "VOID",
        "PVOID": "VOID",
        "LPSTR": "CHAR",
        "LPCSTR": "CONST_CHAR",
        "LPWSTR": "WCHAR",
        "LPCWSTR": "CONST_WCHAR",
        "HANDLE": "VOID",  # HANDLE通常就是VOID*
        "HWND": "VOID",
        "HMODULE": "VOID",
        "HKEY": "VOID",
        "HDC": "VOID",
        "HBRUSH": "VOID",
        "HPEN": "VOID",
        "HFONT": "VOID",
        "HPALETTE": "VOID",
        "HICON": "VOID",
        "HCURSOR": "VOID",
        "DWORD": "UNSIGNED_LONG",
        "WORD": "UNSIGNED_SHORT",
        "BYTE": "UNSIGNED_CHAR",
        "INT": "INT_TYPE",
        "BOOL": "BOOL_TYPE",
        "UINT": "UNSIGNED_INT",
        "ULONG": "UNSIGNED_LONG",
        "ULONGLONG": "UNSIGNED_LONG_LONG",
        "HRESULT": "LONG",
        "NTSTATUS": "LONG",
    }
    
    @staticmethod
    def normalize_type_string(type_str):
        """规范化类型字符串"""
        if not type_str:
            return None
        
        type_str = re.sub(r"[\s\*\&]+", " ", type_str).strip()
        type_str = re.sub(r"(const|volatile|unsigned|signed|struct|enum)\s+", "", type_str)
        
        # 检查别名
        for alias, canonical in TypeAliasNormalizer.WIN32_ALIASES.items():
            if re.search(rf"\b{alias}\b", type_str):
                return canonical
        
        return type_str.split()[0] if type_str else None


# ============================================================================
# 第3部分: Callback/Interface启发式规则
# ============================================================================

class CallbackInterfaceLinker:
    """连接callback和interface到相关类型"""
    
    @staticmethod
    def link_callbacks(name_to_info, edges, existing_edges, dry_run=False):
        """连接callback到其返回类型和参数类型"""
        new_edges = []
        
        # 从name_to_info中找出所有callback
        for name, infos in name_to_info.items():
            for info in infos:
                if info["entity_type"] != "callback":
                    continue
                
                # callback通常命名为 *Callback、*Handler、*Proc等
                # 尝试从名称推断相关类型
                
                # 策略: 查找包含"Callback"的同前缀结构体/枚举
                prefix = re.sub(r"(Callback|Handler|Proc|Function)$", "", name)
                
                if len(prefix) > 3:
                    for candidate_name, candidate_infos in name_to_info.items():
                        if candidate_name.startswith(prefix):
                            for candidate_info in candidate_infos:
                                if candidate_info["entity_type"] in ("struct", "structure", "enum"):
                                    edge_key = (name, candidate_name, "relates_to")
                                    if edge_key not in existing_edges:
                                        edges.append({
                                            "source": name,
                                            "target": candidate_name,
                                            "type": "relates_to",
                                            "_v43_strategy": "callback_prefix_matching"
                                        })
                                        existing_edges.add(edge_key)
                                        new_edges.append(edges[-1])
        
        return new_edges
    
    @staticmethod
    def link_interfaces(name_to_info, edges, existing_edges, dry_run=False):
        """连接interface到其方法"""
        new_edges = []
        
        # 从name_to_info中找出所有interface
        for name, infos in name_to_info.items():
            for info in infos:
                if info["entity_type"] != "interface":
                    continue
                
                # 查找以接口名称为前缀的方法
                method_prefix = name + "_"
                
                for candidate_name, candidate_infos in name_to_info.items():
                    if candidate_name.startswith(method_prefix):
                        for candidate_info in candidate_infos:
                            if candidate_info["entity_type"] == "method":
                                edge_key = (name, candidate_name, "has_method")
                                if edge_key not in existing_edges:
                                    edges.append({
                                        "source": name,
                                        "target": candidate_name,
                                        "type": "has_method",
                                        "_v43_strategy": "interface_method_linking"
                                    })
                                    existing_edges.add(edge_key)
                                    new_edges.append(edges[-1])
        
        return new_edges


# ============================================================================
# 第4部分: 参数类型深度解析
# ============================================================================

class ComplexTypeParser:
    """深度解析复杂参数类型"""
    
    @staticmethod
    def extract_all_types(type_str):
        """从复杂类型声明中提取所有可能的类型"""
        if not type_str:
            return []
        
        types = set()
        
        # 去除指针、const、volatile等
        cleaned = re.sub(r"[\*\&\[\]]+", " ", str(type_str))
        cleaned = re.sub(r"\b(const|volatile|unsigned|signed|struct|enum|union|typedef)\b", " ", cleaned)
        
        # 提取所有identifier
        for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
            if len(token) > 2:  # 避免太短的单词
                types.add(token)
        
        return list(types)
    
    @staticmethod
    def improve_function_parameter_linking(name_to_info, edges, existing_edges, dry_run=False):
        """增强函数参数与类型的连接"""
        new_edges = []
        files = sorted(
            f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
            if not os.path.basename(f).startswith("_")
            and not os.path.basename(f).startswith("global")
        )
        
        for fp in files:
            try:
                data = json.load(open(fp, 'r', encoding='utf-8'))
            except:
                continue
            
            source_file = os.path.basename(fp)
            
            for ent in data.get("entities", []):
                et = str(ent.get("entity_type", "")).strip().lower()
                if et != "function":
                    continue
                
                func_name = ent.get("name", "").strip()
                if not func_name:
                    continue
                
                # 深度解析参数
                for param in ent.get("parameters", []) or []:
                    param_type = param.get("type", "")
                    
                    # 提取所有可能的类型
                    extracted_types = ComplexTypeParser.extract_all_types(param_type)
                    
                    for type_candidate in extracted_types:
                        # 规范化
                        normalized = TypeAliasNormalizer.normalize_type_string(type_candidate)
                        
                        # 查找匹配的实体
                        for target_name, target_infos in name_to_info.items():
                            if target_name.lower() != normalized.lower() if normalized else False:
                                continue
                            
                            for target_info in target_infos:
                                if target_info["entity_type"] in ("struct", "structure", "enum", "typedef"):
                                    edge_key = (func_name, target_name, "uses_type_deep")
                                    if edge_key not in existing_edges:
                                        edges.append({
                                            "source": func_name,
                                            "target": target_name,
                                            "type": "uses_type_deep",
                                            "source_file": source_file,
                                            "_v43_strategy": "complex_type_parsing",
                                            "detail": f"param: {param_type}"
                                        })
                                        existing_edges.add(edge_key)
                                        new_edges.append(edges[-1])
        
        return new_edges


# ============================================================================
# 第5部分: 质量评估
# ============================================================================

class QualityAssessor:
    """评估新增边的质量"""
    
    @staticmethod
    def sample_new_edges_for_review(edges, sample_size=200):
        """采样新增边用于人工审查"""
        new_edges = [e for e in edges if "_v43_strategy" in e or "_v42_strategy" in e]
        
        if len(new_edges) <= sample_size:
            sample = new_edges
        else:
            sample = random.sample(new_edges, sample_size)
        
        report = {
            "total_new_edges": len(new_edges),
            "sample_size": len(sample),
            "sample": []
        }
        
        # 分类统计
        by_strategy = defaultdict(int)
        by_type = defaultdict(int)
        
        for edge in sample:
            strategy = edge.get("_v43_strategy") or edge.get("_v42_strategy", "unknown")
            edge_type = edge.get("type", "unknown")
            
            by_strategy[strategy] += 1
            by_type[edge_type] += 1
            
            report["sample"].append({
                "source": edge.get("source"),
                "target": edge.get("target"),
                "type": edge_type,
                "strategy": strategy,
                "source_file": edge.get("source_file", ""),
                "detail": edge.get("detail", "")
            })
        
        report["by_strategy"] = dict(by_strategy)
        report["by_type"] = dict(by_type)
        
        return report


# ============================================================================
# 主函数
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="KG v4.3: 孤立节点补救增强")
    parser.add_argument("--strategy", choices=["all", "enum-values", "type-aliases", "callback-interface", "complex-types"], 
                       default="all", help="要应用的策略")
    parser.add_argument("--apply", action="store_true", help="实际应用修改")
    parser.add_argument("--quality-check", action="store_true", help="生成质量评估报告")
    parser.add_argument("--sample", type=int, default=200, help="采样大小")
    args = parser.parse_args()
    
    dry_run = not args.apply
    
    if not os.path.isdir(OUT_DIR):
        raise SystemExit(f"json_output_v4 目录不存在: {OUT_DIR}")
    
    print("=" * 70)
    print("[v4.3] 孤立节点补救增强 - 五大高价值改进")
    print("=" * 70)
    
    # 加载现有数据
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )
    
    entities = {}
    name_to_info = defaultdict(list)
    edges = []
    
    # 加载现有实体
    for fp in files:
        try:
            data = json.load(open(fp, 'r', encoding='utf-8'))
            for ent in data.get("entities", []):
                eid = ent.get("id", "").strip()
                name = ent.get("name", "").strip()
                et = str(ent.get("entity_type", "unknown")).strip().lower()
                
                if eid:
                    entities[eid] = {"name": name, "entity_type": et}
                    name_to_info[name].append({"id": eid, "entity_type": et})
        except:
            pass
    
    # 加载现有边
    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    if os.path.exists(edges_path):
        try:
            edoc = json.load(open(edges_path, 'r', encoding='utf-8'))
            edges = edoc.get("edges", [])
        except:
            pass
    
    existing_edges = set()
    for e in edges:
        src = e.get("source", "")
        tgt = e.get("target", "")
        et = e.get("type", "")
        if src and tgt:
            existing_edges.add((src, tgt, et))
    
    print(f"[v4.3] 已加载实体: {len(entities)}, 边: {len(edges)}")
    
    total_new = 0
    
    # 策略1: Enum值生成
    if args.strategy in ("all", "enum-values"):
        print("\n[v4.3] 策略1: Enum值实体动态生成 ...")
        new_ents, new_edges_list, count = EnumValueGenerator.generate_from_enums(files, entities, edges, dry_run)
        print(f"       新增实体: {len(new_ents)}, 新增边: {count}")
        total_new += count
    
    # 策略2: 类型别名（这个主要用于后续匹配）
    if args.strategy in ("all", "type-aliases"):
        print("\n[v4.3] 策略2: 类型别名规范化 ...")
        print("       类型别名映射已加载 (用于参数匹配)")
    
    # 策略3: Callback/Interface
    if args.strategy in ("all", "callback-interface"):
        print("\n[v4.3] 策略3: Callback/Interface链接 ...")
        new_cb = CallbackInterfaceLinker.link_callbacks(name_to_info, edges, existing_edges, dry_run)
        new_if = CallbackInterfaceLinker.link_interfaces(name_to_info, edges, existing_edges, dry_run)
        print(f"       新增callback边: {len(new_cb)}")
        print(f"       新增interface边: {len(new_if)}")
        total_new += len(new_cb) + len(new_if)
    
    # 策略4: 复杂类型解析
    if args.strategy in ("all", "complex-types"):
        print("\n[v4.3] 策略4: 参数类型深度解析 ...")
        new_complex = ComplexTypeParser.improve_function_parameter_linking(name_to_info, edges, existing_edges, dry_run)
        print(f"       新增边: {len(new_complex)}")
        total_new += len(new_complex)
    
    print(f"\n[v4.3] 总新增边数: {total_new}")
    print(f"[v4.3] 新总边数: {len(edges)}")
    
    # 质量评估
    if args.quality_check:
        print(f"\n[v4.3] 生成质量评估报告 (采样 {args.sample} 条)...")
        report = QualityAssessor.sample_new_edges_for_review(edges, args.sample)
        
        report_path = os.path.join(OUT_DIR, "_v43_quality_assessment.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n质量评估报告:")
        print(f"  新增边总数: {report['total_new_edges']}")
        print(f"  采样大小: {report['sample_size']}")
        print(f"\n按策略分布:")
        for strategy, count in report['by_strategy'].items():
            print(f"  {strategy}: {count}")
        print(f"\n按边类型分布:")
        for et, count in report['by_type'].items():
            print(f"  {et}: {count}")
        print(f"\n已保存: {report_path}")
    
    if dry_run:
        print("\n[v4.3] 干运行模式 - 未实际修改文件")
        print("       使用 --apply 标志进行实际应用")
    else:
        out_path = os.path.join(OUT_DIR, "global_edges_v43.json")
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump({"edges": edges}, f, ensure_ascii=False, indent=2)
        print(f"\n[v4.3] 已写入: {out_path}")


if __name__ == "__main__":
    main()
