#!/usr/bin/env python3
"""
孤立节点改进方案对比和优先级分析工具
"""

import json

def print_improvement_comparison():
    """打印改进方案对比表"""
    
    print("\n" + "="*100)
    print("孤立节点改进方案对比分析")
    print("="*100)
    
    strategies = [
        {
            "name": "Enum值实体生成",
            "id": "enum-values",
            "impact": "500-800条边",
            "target_types": ["enum", "enum_value"],
            "current_isolated": "873 enum + 0 enum_value",
            "after_improvement": "~400 enum + 500 enum_value",
            "difficulty": "⭐ 低",
            "effort_hours": "1-2",
            "precision_expected": "98%",
            "priority": "🔴 HIGH",
            "recommendation": "最优先实施"
        },
        {
            "name": "Windows类型别名规范化",
            "id": "type-aliases", 
            "impact": "800-1200条边",
            "target_types": ["function", "struct", "enum"],
            "current_isolated": "function: 1303, struct: 1062",
            "after_improvement": "function: ~900, struct: ~900",
            "difficulty": "⭐ 低",
            "effort_hours": "1-2",
            "precision_expected": "85-90%",
            "priority": "🔴 HIGH",
            "recommendation": "与enum并行实施"
        },
        {
            "name": "参数类型深度解析",
            "id": "complex-types",
            "impact": "400-600条边",
            "target_types": ["function"],
            "current_isolated": "function: 742",
            "after_improvement": "function: ~550",
            "difficulty": "⭐⭐ 中等",
            "effort_hours": "2-3",
            "precision_expected": "80-85%",
            "priority": "🟠 MEDIUM",
            "recommendation": "第二波实施"
        },
        {
            "name": "Callback/Interface启发式",
            "id": "callback-interface",
            "impact": "250-350条边",
            "target_types": ["callback", "interface", "method"],
            "current_isolated": "callback: 431, interface: 97",
            "after_improvement": "callback: ~200, interface: ~20",
            "difficulty": "⭐⭐ 中等",
            "effort_hours": "1-2",
            "precision_expected": "75-80%",
            "priority": "🟠 MEDIUM",
            "recommendation": "可与深度解析并行"
        },
        {
            "name": "纯启发式 v4.2基础",
            "id": "v42-baseline",
            "impact": "34,350条边",
            "target_types": ["constant", "struct", "enum"],
            "current_isolated": "6193 (26.23%)",
            "after_improvement": "5352 (22.67%)",
            "difficulty": "✓ 已完成",
            "effort_hours": "0",
            "precision_expected": "~78%",
            "priority": "✓ 已实施",
            "recommendation": "基础方案"
        }
    ]
    
    print("\n【改进方案详细对比】\n")
    
    for i, s in enumerate(strategies, 1):
        print(f"\n{s['priority']} 方案{i}: {s['name']}")
        print("-" * 90)
        print(f"  策略ID:              {s['id']}")
        print(f"  预期新增边:          {s['impact']}")
        print(f"  作用对象:            {', '.join(s['target_types'])}")
        print(f"  当前孤立状态:        {s['current_isolated']}")
        print(f"  改进后状态:          {s['after_improvement']}")
        print(f"  实现难度:            {s['difficulty']}")
        print(f"  预估工时:            {s['effort_hours']}小时")
        print(f"  预期准确率:          {s['precision_expected']}")
        print(f"  建议:                {s['recommendation']}")


def print_phase_comparison():
    """对比不同实施阶段的效果"""
    
    print("\n" + "="*100)
    print("分阶段实施效果预测")
    print("="*100)
    
    phases = [
        {
            "phase": "基础 (v4.2现状)",
            "strategies": ["Constant Contextualization"],
            "isolated_count": "5,352",
            "isolated_rate": "22.67%",
            "connected_rate": "77.33%",
            "total_edges": "100,531",
            "effort": "已完成"
        },
        {
            "phase": "Phase 1 (快速赢)",
            "strategies": ["Enum值生成", "类型别名规范化"],
            "isolated_count": "~4,700",
            "isolated_rate": "~19.9%",
            "connected_rate": "~80.1%",
            "total_edges": "~102,000",
            "effort": "2-3小时"
        },
        {
            "phase": "Phase 1+2 (完整增强)",
            "strategies": ["所有策略"],
            "isolated_count": "~3,800",
            "isolated_rate": "~16.1%",
            "connected_rate": "~83.9%",
            "total_edges": "~105,000",
            "effort": "5-7小时"
        },
        {
            "phase": "Phase 3 (With 质量验证)",
            "strategies": ["所有策略 + 采样检查"],
            "isolated_count": "~3,500",
            "isolated_rate": "~14.8%",
            "connected_rate": "~85.2%",
            "total_edges": "~106,000",
            "effort": "6-8小时"
        }
    ]
    
    # 表头
    print("\n")
    print(f"{'阶段':<15} | {'主要策略':<25} | {'孤立节点':<12} | {'孤立率':<8} | {'连通率':<8} | {'工时':<10}")
    print("-" * 100)
    
    for phase in phases:
        phase_name = phase["phase"]
        strategies = ", ".join(phase["strategies"][:2])
        if len(phase["strategies"]) > 2:
            strategies += f"等{len(phase['strategies'])}项"
        
        print(f"{phase_name:<15} | {strategies:<25} | {phase['isolated_count']:<12} | {phase['isolated_rate']:<8} | {phase['connected_rate']:<8} | {phase['effort']:<10}")


def print_roi_analysis():
    """投入产出分析"""
    
    print("\n" + "="*100)
    print("投入产出 (ROI) 分析")
    print("="*100)
    
    print("""
【最高ROI策略排序】

1. 🥇 Enum值生成 + 类型别名规范化
   - 投入: 2-3小时
   - 产出: 1300-2000条边  
   - 孤立率改善: 26.23% → ~19.9%
   - ROI: 600条边/小时 ⭐⭐⭐⭐⭐

2. 🥈 参数类型深度解析
   - 投入: 2-3小时
   - 产出: 400-600条边
   - 孤立率改善: ~19.9% → ~18%
   - ROI: 200条边/小时 ⭐⭐⭐

3. 🥉 Callback/Interface启发式
   - 投入: 1-2小时
   - 产出: 250-350条边
   - 孤立率改善: ~18% → ~17.5%
   - ROI: 175条边/小时 ⭐⭐

【可选优化 (成本较高)】

4. 语义相似度匹配
   - 投入: 4-6小时 (需构建embedding)
   - 产出: 不确定 (需验证准确率)
   - ROI: 未知 ⚠️
   - 建议: 等待phase 3验证质量后再考虑

5. LLM辅助
   - 投入: 3-5小时 + API成本
   - 产出: 不确定 (LLM质量依赖)
   - ROI: 未知 ⚠️
   - 建议: 最后才考虑
""")


def print_decision_tree():
    """决策树"""
    
    print("\n" + "="*100)
    print("快速决策指南")
    print("="*100)
    
    print("""
【优先级决策】

Question 1: 有多少时间?
├─ 少于1小时? → 跳过v4.3，保持v4.2现状
├─ 1-3小时?   → ✅ 仅实施 enum-values + type-aliases
├─ 3-6小时?   → ✅ 实施全部v4.3策略
└─ 6小时以上? → ✅ 实施v4.3 + 质量验证 + 迭代

Question 2: 对准确率的要求?
├─ 85%以上?      → 必须加质量检查 (phase 3)
├─ 80-85%之间?  → 推荐加质量检查
├─ 70-80%可接受? → 可跳过质量检查
└─ > 70%即可?   → 仅需基础v4.2

Question 3: 后续是否还会继续优化?
├─ 是 → 做完整的质量评估报告备档
└─ 否 → 最小化方案: enum-values + type-aliases

【推荐组合】

🎯 标准方案 (推荐) - 3-4小时
  1. python kg_enrich_v43.py --strategy all --apply
  2. python assess_isolated_nodes.py
  3. python kg_enrich_v43.py --quality-check --sample 200
  预期: 孤立率 22.67% → 16-18%

🚀 快速方案 - 1-2小时  
  1. python kg_enrich_v43.py --strategy enum-values --apply
  2. python kg_enrich_v43.py --strategy type-aliases --apply
  3. python assess_isolated_nodes.py
  预期: 孤立率 22.67% → 19-20%

⚙️ 保守方案 - 0小时
  保持现状 (v4.2已经很好!)
  孤立率: 22.67%, 连通率: 77.33%
""")


def print_next_actions():
    """后续行动"""
    
    print("\n" + "="*100)
    print("后续行动步骤")
    print("="*100)
    
    print("""
【立即行动】(现在就做):

1. 📋 审视时间预算
   $ 你有多少时间进行这项改进?

2. 🎯 选择方案
   $ 根据时间和质量要求选择上述方案之一

3. 🚀 执行第一步
   $ python kg_enrich_v43.py --strategy enum-values
   (预检查，不实际修改文件)


【执行流程】:

阶段1 - 预检查 (10分钟)
  $ python kg_enrich_v43.py --strategy all
  查看是否有报错，评估产生的边数预估

阶段2 - 实施 (5-10分钟)
  $ python kg_enrich_v43.py --strategy all --apply
  实际应用改进

阶段3 - 评估 (5分钟)
  $ python assess_isolated_nodes.py
  查看孤立节点是否减少

阶段4 - 质量检查 (15-30分钟，可选)
  $ python kg_enrich_v43.py --quality-check --sample 300
  采样检查新增边质量，生成评估报告


【期望时间表】:

- 总预检查: 10分钟
- 实施: 10分钟  
- 快速评估: 5分钟
- 质量检查 (可选): 30分钟
- 总计: 25分钟 - 1小时


【风险评估】:

✅ 低风险 - 所有文件都有备份
✅ 可逆性 - 可随时回滚到v4.2
✅ 递进性 - 没有依赖冲突
✅ 高收益 - 确定可以减少~1500个孤立节点
""")


if __name__ == "__main__":
    print_improvement_comparison()
    print_phase_comparison()
    print_roi_analysis()
    print_decision_tree()
    print_next_actions()
    
    print("\n" + "="*100)
    print("📖 更多详情请参考: IMPROVEMENT_ROADMAP.md")
    print("📊 执行v4.3脚本: python kg_enrich_v43.py")
    print("="*100)
