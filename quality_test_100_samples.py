#!/usr/bin/env python3
"""
LLM 配置质量评估测试：100 样本
保留之前的测试结果，生成详细质量报告
"""

import os
import sys
import json
import random
import asyncio
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline import (
    KnowledgeGraph, SYSTEM_PROMPT, build_user_prompt, 
    parse_llm_response, AsyncRateLimiter, LLMClient,
    load_llm_config
)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")
LLM_CKPT_FILE = os.path.join(OUT_DIR, "_llm_checkpoint.json")
LOGS_DIR = os.path.join(THIS_DIR, "logs")

def load_checkpoint_entities():
    """从断点文件加载已处理的实体 ID 列表"""
    if not os.path.exists(LLM_CKPT_FILE):
        print("✗ 断点文件不存在")
        return []
    
    with open(LLM_CKPT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data.get('processed_ids', [])

def load_graph():
    """加载知识图谱"""
    graph = KnowledgeGraph()
    graph.load_from_dir(OUT_DIR)
    return graph

def select_stratified_samples(processed_ids, graph, n=100):
    """分层抽样：按 entity_type 分布选取样本"""
    # 统计各类型数量
    type_counts = {}
    for eid in processed_ids:
        ent = graph.entities.get(eid)
        if ent:
            et = ent.get('entity_type', 'unknown')
            type_counts[et] = type_counts.get(et, 0) + 1
    
    # 按比例分配样本数
    total = sum(type_counts.values())
    type_samples = {}
    remaining = n
    
    for et, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        ratio = count / total
        allocated = max(1, int(n * ratio))
        if allocated > count:
            allocated = count
        type_samples[et] = min(allocated, count)
        remaining -= allocated
    
    # 分配剩余名额给大类型
    for et in sorted(type_counts.keys(), key=lambda x: -type_counts[x]):
        if remaining <= 0:
            break
        can_add = type_counts[et] - type_samples.get(et, 0)
        add = min(can_add, remaining)
        type_samples[et] = type_samples.get(et, 0) + add
        remaining -= add
    
    # 按类型抽样
    type_to_ids = {}
    for eid in processed_ids:
        ent = graph.entities.get(eid)
        if ent:
            et = ent.get('entity_type', 'unknown')
            if et not in type_to_ids:
                type_to_ids[et] = []
            type_to_ids[et].append(eid)
    
    sample_ids = []
    for et, count in type_samples.items():
        ids = type_to_ids.get(et, [])
        if len(ids) <= count:
            sample_ids.extend(ids)
        else:
            sample_ids.extend(random.sample(ids, count))
    
    random.shuffle(sample_ids)
    return sample_ids, type_samples

async def test_with_llm(graph, entity_id, llm_config, provider_name, semaphore):
    """用指定 LLM 测试单个实体"""
    async with semaphore:
        ent = graph.entities.get(entity_id)
        if not ent:
            return None
        
        nb, edges = graph.get_neighborhood(entity_id)
        prompt = build_user_prompt(ent, nb, edges)
        
        rl = AsyncRateLimiter(llm_config.requests_per_min)
        async with LLMClient(llm_config, rl) as client:
            start = time.time()
            raw = await client.chat([
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': prompt}
            ])
            elapsed = time.time() - start
            
            original_type = ent.get('entity_type', 'unknown')
            
            if raw:
                parsed = parse_llm_response(raw) or {
                    'verdict': 'keep',
                    'confidence': 0.0,
                    'summary': '解析失败',
                    'operations': []
                }
                
                # 分析操作类型
                ops = parsed.get('operations', [])
                op_types = [op.get('op') for op in ops]
                
                # 判断是否需要修改
                has_update = any(op.get('op') == 'update_field' for op in ops)
                has_delete_edge = any(op.get('op') == 'delete_edge' for op in ops)
                has_add_edge = any(op.get('op') == 'add_edge' for op in ops)
                has_delete_node = any(op.get('op') == 'delete_node' for op in ops)
                
                # 提取修改的字段
                updated_fields = [op.get('field') for op in ops if op.get('op') == 'update_field']
                
                # 判断 entity_type 是否被修正
                type_corrected = False
                for op in ops:
                    if op.get('op') == 'update_field' and op.get('field') == 'entity_type':
                        type_corrected = True
                        break
                
                return {
                    'entity_id': entity_id,
                    'entity_name': ent.get('name', ''),
                    'original_entity_type': original_type,
                    'provider': provider_name,
                    'elapsed': elapsed,
                    'raw_length': len(raw),
                    'parsed': parsed,
                    'operations_count': len(ops),
                    'has_update': has_update,
                    'has_delete_edge': has_delete_edge,
                    'has_add_edge': has_add_edge,
                    'has_delete_node': has_delete_node,
                    'type_corrected': type_corrected,
                    'updated_fields': updated_fields,
                    'verdict': parsed.get('verdict', 'unknown'),
                    'confidence': parsed.get('confidence', 0),
                }
            else:
                return {
                    'entity_id': entity_id,
                    'entity_name': ent.get('name', ''),
                    'provider': provider_name,
                    'elapsed': elapsed,
                    'error': '返回为空',
                    'original_entity_type': original_type,
                }

async def run_quality_test(sample_ids, graph, type_distribution):
    """运行质量测试"""
    qwen_config = load_llm_config('deepseek')
    if not qwen_config:
        print("✗ Qwen 配置加载失败")
        return []
    
    print(f"\n新配置：{qwen_config.model_name} @ {qwen_config.api_base_url}", flush=True)
    print(f"样本数：{len(sample_ids)}", flush=True)
    print(f"类型分布：{type_distribution}", flush=True)
    print()
    
    results = []
    semaphore = asyncio.Semaphore(qwen_config.max_workers)
    
    for i, eid in enumerate(sample_ids, 1):
        print(f"[{i}/{len(sample_ids)}] 测试：{eid}", end=" ", flush=True)
        result = await test_with_llm(graph, eid, qwen_config, "Qwen3.5-27B", semaphore)
        if result:
            results.append(result)
            print(f"✓ {result['elapsed']:.1f}s | {result.get('verdict', 'N/A')} | conf={result.get('confidence', 0):.2f}", flush=True)
        else:
            print(f"✗ 失败", flush=True)
    
    return results

def analyze_quality(results, type_distribution):
    """质量分析"""
    total = len(results)
    success = sum(1 for r in results if 'parsed' in r and 'error' not in r)
    
    # 基础统计
    avg_time = sum(r['elapsed'] for r in results) / total if total else 0
    avg_confidence = sum(r.get('confidence', 0) for r in results if 'confidence' in r) / success if success else 0
    
    # 裁决分布
    verdicts = {}
    for r in results:
        v = r.get('verdict', 'unknown')
        verdicts[v] = verdicts.get(v, 0) + 1
    
    # 操作类型统计
    op_stats = {
        'has_update': sum(1 for r in results if r.get('has_update', False)),
        'has_delete_edge': sum(1 for r in results if r.get('has_delete_edge', False)),
        'has_add_edge': sum(1 for r in results if r.get('has_add_edge', False)),
        'has_delete_node': sum(1 for r in results if r.get('has_delete_node', False)),
    }
    
    # 字段修改统计
    field_update_counts = {}
    for r in results:
        for field in r.get('updated_fields', []):
            field_update_counts[field] = field_update_counts.get(field, 0) + 1
    
    # 类型修正统计
    type_corrected_count = sum(1 for r in results if r.get('type_corrected', False))
    
    # 按原始类型分组统计
    type_stats = {}
    for r in results:
        orig_type = r.get('original_entity_type', 'unknown')
        if orig_type not in type_stats:
            type_stats[orig_type] = {'total': 0, 'success': 0, 'corrected': 0, 'avg_conf': 0, 'conf_sum': 0}
        type_stats[orig_type]['total'] += 1
        if 'parsed' in r and 'error' not in r:
            type_stats[orig_type]['success'] += 1
            type_stats[orig_type]['conf_sum'] += r.get('confidence', 0)
        if r.get('type_corrected', False):
            type_stats[orig_type]['corrected'] += 1
    
    for et in type_stats:
        if type_stats[et]['success'] > 0:
            type_stats[et]['avg_conf'] = type_stats[et]['conf_sum'] / type_stats[et]['success']
        del type_stats[et]['conf_sum']
    
    return {
        'total': total,
        'success': success,
        'success_rate': success / total * 100 if total else 0,
        'avg_time': avg_time,
        'avg_confidence': avg_confidence,
        'verdicts': verdicts,
        'op_stats': op_stats,
        'field_update_counts': field_update_counts,
        'type_corrected_count': type_corrected_count,
        'type_corrected_rate': type_corrected_count / total * 100 if total else 0,
        'type_stats': type_stats,
    }

def generate_report(results, analysis, type_distribution, prev_results):
    """生成详细报告"""
    report = {
        'test_info': {
            'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'model': 'Qwen/Qwen3.5-27B',
            'api_base': 'http://100.122.242.51:8000/v1',
            'total_samples': len(results),
            'type_distribution': type_distribution,
        },
        'quality_analysis': analysis,
        'previous_10_sample_test': prev_results,
        'detailed_results': results,
    }
    
    # 保存报告
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(LOGS_DIR, f"llm_quality_100_samples_{timestamp}.json")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("质量评估报告摘要")
    print("=" * 70)
    print(f"\n测试时间：{report['test_info']['test_time']}")
    print(f"模型：{report['test_info']['model']}")
    print(f"样本数：{len(results)}")
    print()
    print(f"✓ 成功解析：{analysis['success']}/{analysis['total']} ({analysis['success_rate']:.1f}%)")
    print(f"✓ 平均耗时：{analysis['avg_time']:.2f}秒/实体")
    print(f"✓ 平均置信度：{analysis['avg_confidence']:.2f}")
    print()
    print(f"裁决分布：{analysis['verdicts']}")
    print(f"类型修正率：{analysis['type_corrected_rate']:.1f}% ({analysis['type_corrected_count']}/{analysis['total']})")
    print()
    print("操作统计:")
    for op, count in analysis['op_stats'].items():
        print(f"  {op}: {count} ({count/analysis['total']*100:.1f}%)")
    print()
    print("字段修改统计:")
    for field, count in sorted(analysis['field_update_counts'].items(), key=lambda x: -x[1]):
        print(f"  {field}: {count}")
    print()
    print("按类型统计:")
    for et, stats in sorted(analysis['type_stats'].items(), key=lambda x: -x[1]['total']):
        print(f"  {et}: {stats['total']}样本，{stats['success']}成功，{stats['corrected']}修正，avg_conf={stats['avg_conf']:.2f}")
    print()
    print(f"完整报告：{report_path}")
    
    return report_path

async def main():
    print("=" * 70)
    print("LLM 质量评估测试：100 样本")
    print("=" * 70)
    
    # 加载数据
    processed_ids = load_checkpoint_entities()
    print(f"已处理实体：{len(processed_ids)} 个")
    
    graph = load_graph()
    print(f"图谱加载完成：{len(graph.entities)} 实体")
    
    # 分层抽样
    sample_ids, type_distribution = select_stratified_samples(processed_ids, graph, n=100)
    print(f"随机样本：{len(sample_ids)} 个")
    
    # 运行测试
    results = await run_quality_test(sample_ids, graph, type_distribution)
    
    # 加载之前的 10 样本测试结果
    prev_results_path = os.path.join(LOGS_DIR, "llm_comparison_20260315_104738.json")
    prev_results = None
    if os.path.exists(prev_results_path):
        with open(prev_results_path, 'r', encoding='utf-8') as f:
            prev_results = json.load(f)
    
    # 分析质量
    analysis = analyze_quality(results, type_distribution)
    
    # 生成报告
    report_path = generate_report(results, analysis, type_distribution, prev_results)
    
    print("\n" + "=" * 70)
    print("测试完成!")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())
