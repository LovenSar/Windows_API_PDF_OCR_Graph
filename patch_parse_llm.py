#!/usr/bin/env python3
"""
增强 parse_llm_response 函数的补丁
支持更多思考标签格式和中文思考文本的解析
"""

import re
import json

# 读取原文件
with open('/Users/lovensar/Workspace/Windows_API_PDF/pipeline.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 新的 parse_llm_response 函数
new_func = '''def parse_llm_response(raw_text):
    """解析 LLM 响应，支持多种思考标签格式"""
    if not raw_text: return None
    
    text = raw_text.strip()
    
    # 清理各种思考过程标签
    # 格式 1: <think>...</think>
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()
    # 格式 2: <thought>...</thought>
    text = re.sub(r"<thought>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()
    # 格式 3: <reasoning>...</reasoning>
    text = re.sub(r"<reasoning>.*?</reasoning>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()
    # 格式 4: 中文"好的，我现在需要..."开头的长段思考（直到遇到 JSON 结构）
    if text.startswith("好的") or text.startswith("让我") or text.startswith("首先") or text.startswith("我"):
        # 查找第一个 { 的位置
        brace_idx = text.find("{")
        if brace_idx > 0 and brace_idx < 800:
            text = text[brace_idx:]
    
    obj = _robust_json_load(text)
    if isinstance(obj, dict):
        return _validate_response(obj)
    
    fb_obj = _fallback_parse_response_from_text(text)
    if isinstance(fb_obj, dict):
        return _validate_response(fb_obj)
    
    # 最后尝试：从文本中直接抽取关键字段
    fb_obj2 = _extract_fields_from_thought(text)
    if fb_obj2:
        return _validate_response(fb_obj2)
    
    log.warning(f"无法解析 LLM 响应：{text[:300]}...")
    return None

def _extract_fields_from_thought(text):
    """从思考文本中提取关键字段（最后手段）"""
    # 尝试查找 verdict
    verdict_match = re.search(r'裁决 [：:]\\s*(keep|delete|merge|保留 | 删除 | 合并)', text, re.I)
    verdict = 'keep'
    if verdict_match:
        v = verdict_match.group(1).lower()
        if '删' in v or v == 'delete': verdict = 'delete'
        elif '合' in v or v == 'merge': verdict = 'merge'
        else: verdict = 'keep'
    
    # 尝试查找 confidence
    conf_match = re.search(r'置信度 [：:]\\s*([0-9.]+)', text, re.I)
    conf = 0.5
    if conf_match:
        try: conf = float(conf_match.group(1))
        except: pass
    
    # 尝试查找 summary（找总结性的话）
    summary_match = re.search(r'(?:总结 | 摘要 | 结论 | 综上 | 判断 | 因此)[:：\\s]+([^\\n。]+)', text, re.I)
    summary = ''
    if summary_match:
        summary = summary_match.group(1).strip()[:200]
    else:
        # 找包含"实体"的关键句子
        sent_match = re.search(r'实体 [^\\n。]{0,150}', text)
        if sent_match:
            summary = sent_match.group(0)[:200]
    
    if verdict or conf != 0.5 or summary:
        return {'verdict': verdict, 'confidence': conf, 'summary': summary, 'operations': []}
    
    return None

'''

# 找到旧函数并替换
old_func_pattern = r'def parse_llm_response\\(raw_text\\):\\s+if not raw_text: return None\\s+text = re\\.sub\\(r"<think>\\.\\*\\?</think>","",raw_text\\.strip\\(\\),flags=re\\.DOTALL\\)\\.strip\\(\\)\\s+obj = _robust_json_load\\(text\\)\\s+if isinstance\\(obj, dict\\):\\s+return _validate_response\\(obj\\)\\s+fb_obj = _fallback_parse_response_from_text\\(text\\)\\s+if isinstance\\(fb_obj, dict\\):\\s+return _validate_response\\(fb_obj\\)\\s+log\\.warning\\(f"无法解析 LLM 响应：\\{text\\[:200\\]\\}\\.\\.\\."\)\\s+return None'

# 使用更简单的方法：直接按行号替换
lines = content.split('\\n')

# 找到函数开始和结束的行
start_line = None
end_line = None
for i, line in enumerate(lines):
    if 'def parse_llm_response(raw_text):' in line:
        start_line = i
    elif start_line is not None and line.strip().startswith('def _validate_response'):
        end_line = i
        break

if start_line is not None and end_line is not None:
    # 替换函数
    new_lines = lines[:start_line] + new_func.strip().split('\\n') + [''] + lines[end_line:]
    
    # 写回文件
    with open('/Users/lovensar/Workspace/Windows_API_PDF/pipeline.py', 'w', encoding='utf-8') as f:
        f.write('\\n'.join(new_lines))
    
    print(f"✓ 成功替换 parse_llm_response 函数")
    print(f"  原函数：行 {start_line+1} 到 {end_line}")
    print(f"  新函数：{len(new_func.split(chr(10)))} 行")
else:
    print(f"✗ 未找到目标函数")
    print(f"  start_line={start_line}, end_line={end_line}")
