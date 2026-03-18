#!/bin/bash
# 断点续跑 LLM 精炼任务
# 使用新配置：Qwen/Qwen3.5-27B @ 100.122.242.51:8000

cd /Users/lovensar/Workspace/Windows_API_PDF

echo "============================================================"
echo "LLM 精炼 - 断点续跑"
echo "============================================================"
echo ""
echo "配置信息:"
echo "  API: http://100.122.242.51:8000/v1"
echo "  模型：Qwen/Qwen3.5-27B"
echo "  超时：600 秒"
echo ""

# 显示断点状态
python3 -c "
import json
with open('json_output_v4/_llm_checkpoint.json', 'r') as f:
    data = json.load(f)
processed = len(data.get('processed_ids', []))
total = 23606
print(f'断点状态:')
print(f'  已处理：{processed} 实体')
print(f'  剩余：{total - processed} 实体')
print(f'  进度：{processed / total * 100:.1f}%')
"

echo ""
echo "按 Ctrl+C 可隨時中斷，進度會自動保存"
echo "============================================================"
echo ""

# 使用 tmux 后台运行（推荐长时间任务）
if command -v tmux &> /dev/null; then
    echo "使用 tmux 后台运行..."
    tmux new-session -d -s kg_refine_resume "python pipeline.py --phase refine --resume"
    echo "任务已在 tmux 会话 'kg_refine_resume' 中启动"
    echo "查看日志：tmux attach -t kg_refine_resume"
    echo "分离会话：按 Ctrl+B 然后按 D"
else
    echo "直接运行（前台模式）..."
    python pipeline.py --phase refine --resume
fi
