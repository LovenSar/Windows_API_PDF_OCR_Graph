#!/usr/bin/env python3
"""
LLM 配置对比测试：GPT-4 vs Qwen3.5-27B
随机选取 10 个已处理实体进行烟测，不回填进度
"""

import os
import sys
import json
import random
import asyncio
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline import (
    KnowledgeGraph, SYSTEM_PROMPT, build_user_prompt, 
    parse_llm_response, AsyncRateLimiter, LLMClient,
    load_llm_config, LLMProviderConfig
)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")
LLM_CKPT_FILE = os.path.join(OUT_DIR, "_llm_checkpoint.json")

# GPT-4 配置（旧）
GPT4_CONFIG = LLMProviderConfig(
    api_base_url="https://svip.xty.app/v1",
    api_key="sk-your_key",  # 占位符，实际已处理过
    model_name="gpt-5.4",
    batch_size=10,
    max_workers=4,
    requests_per_min=60,
    max_retries=3,
    retry_backoff_base=0.5,
    timeout=600
)

def load_checkpoint_entities():
    """从断点文件加载已处理的实体 ID 列表"""
    if not os.path.exists(LLM_CKPT_FILE):
        print("✗ 断点文件不存在")
        return []
    
    with open(LLM_CKPT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    processed_ids = data.get('processed_ids', [])
    print(f"✓ 已处理实体：{len(processed_ids)} 个")
    return processed_ids

def load_graph():
    """加载知识图谱"""
    graph = KnowledgeGraph()
    graph.load_from_dir(OUT_DIR)
    print(f"✓ 图谱加载完成：{len(graph.entities)} 实体")
    return graph

def select_random_samples(processed_ids, n=10):
    """随机选取 N 个样本"""
    if len(processed_ids) < n:
        return processed_ids
    return random.sample(processed_ids, n)

async def test_with_llm(graph, entity_id, llm_config, provider_name):
    """用指定 LLM 测试单个实体"""
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
        
        if raw:
            parsed = parse_llm_response(raw) or {
                'verdict': 'keep',
                'confidence': 0.0,
                'summary': '解析失败',
                'operations': []
            }
            return {
                'entity_id': entity_id,
                'entity_name': ent.get('name', ''),
                'entity_type': ent.get('entity_type', ''),
                'provider': provider_name,
                'elapsed': elapsed,
                'raw': raw[:500] + '...' if len(raw) > 500 else raw,
                'parsed': parsed
            }
        else:
            return {
                'entity_id': entity_id,
                'entity_name': ent.get('name', ''),
                'provider': provider_name,
                'elapsed': elapsed,
                'error': '返回为空'
            }

async def run_comparison(sample_ids, graph):
    """运行对比测试"""
    print("\n" + "=" * 70)
    print("开始对比测试")
    print("=" * 70)
    
    # 只测试新配置（Qwen），因为 GPT-4 的结果已经在断点里了
    qwen_config = load_llm_config('deepseek')
    if not qwen_config:
        print("✗ Qwen 配置加载失败")
        return []
    
    print(f"\n新配置：{qwen_config.model_name} @ {qwen_config.api_base_url}")
    print(f"样本数：{len(sample_ids)}")
    print()
    
    results = []
    for i, eid in enumerate(sample_ids, 1):
        print(f"[{i}/{len(sample_ids)}] 测试：{eid}")
        result = await test_with_llm(graph, eid, qwen_config, "Qwen3.5-27B")
        if result:
            results.append(result)
            print(f"  ✓ 耗时：{result['elapsed']:.2f}s")
            print(f"   verdict: {result['parsed'].get('verdict', 'N/A')}")
            print(f"  confidence: {result['parsed'].get('confidence', 0)}")
            print(f"  summary: {result['parsed'].get('summary', '')[:80]}")
        else:
            print(f"  ✗ 失败")
        print()
    
    return results

def compare_results(results, graph):
    """对比结果分析"""
    print("\n" + "=" * 70)
    print("对比分析")
    print("=" * 70)
    
    # 统计
    total = len(results)
    success = sum(1 for r in results if 'parsed' in r)
    avg_time = sum(r['elapsed'] for r in results) / total if total else 0
    
    verdicts = {}
    confidences = []
    for r in results:
        if 'parsed' in r:
            v = r['parsed'].get('verdict', 'unknown')
            verdicts[v] = verdicts.get(v, 0) + 1
            confidences.append(r['parsed'].get('confidence', 0))
    
    print(f"\n测试完成：{total} 样本")
    print(f"成功解析：{success}/{total}")
    print(f"平均耗时：{avg_time:.2f}s/实体")
    print(f"\n裁决分布：{verdicts}")
    print(f"平均置信度：{sum(confidences)/len(confidences):.2f}" if confidences else "N/A")
    
    # 详细输出
    print("\n" + "-" * 70)
    print("详细结果")
    print("-" * 70)
    for r in results:
        print(f"\n实体：{r['entity_name']} ({r['entity_type']})")
        if 'parsed' in r:
            p = r['parsed']
            print(f"  裁决：{p.get('verdict', 'N/A')}")
            print(f"  置信度：{p.get('confidence', 0)}")
            print(f"  摘要：{p.get('summary', 'N/A')}")
            if p.get('operations'):
                print(f"  操作数：{len(p['operations'])}")
                for op in p['operations'][:3]:
                    print(f"    - {op.get('op', '')}: {op}")
        else:
            print(f"  错误：{r.get('error', '未知')}")

def save_comparison_report(results):
    """保存对比报告"""
    report_path = os.path.join(THIS_DIR, "logs", f"llm_comparison_{time.strftime('%Y%m%d_%H%M%S')}.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    report = {
        'test_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'total_samples': len(results),
        'results': results
    }
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ 报告已保存：{report_path}")

async def main():
    print("=" * 70)
    print("LLM 配置对比测试：GPT-4 vs Qwen3.5-27B")
    print("=" * 70)
    
    # 加载数据
    processed_ids = load_checkpoint_entities()
    if not processed_ids:
        return
    
    graph = load_graph()
    
    # 随机选样
    sample_ids = select_random_samples(processed_ids, n=10)
    print(f"\n随机样本：{sample_ids}")
    
    # 运行测试
    results = await run_comparison(sample_ids, graph)
    
    # 分析结果
    if results:
        compare_results(results, graph)
        save_comparison_report(results)

if __name__ == "__main__":
    asyncio.run(main())
