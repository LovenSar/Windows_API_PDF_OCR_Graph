#!/usr/bin/env bash
# 图谱可视化页面入口 — 从项目根目录启动 graph_viewer

cd "$(dirname "$0")"
DATA_DIR="${1:-json_output_v4}"
OCR_DIR="${2:-}"
PORT=10086

if [[ ! -f "$DATA_DIR/global_entity_index.json" ]]; then
  echo "错误：未找到 $DATA_DIR/global_entity_index.json"
  echo "请先运行 pipeline 生成数据：python pipeline.py"
  exit 1
fi

# 检查端口是否被占用
check_port_in_use() {
  lsof -ti ":$PORT" 2>/dev/null
}

# 启动前检查端口占用
PID=$(check_port_in_use)
if [[ -n "$PID" ]]; then
  echo "警告：端口 $PORT 正被进程 $PID 占用，正在终止..."
  
  # 获取进程信息
  PROCESS_INFO=$(ps -p "$PID" -o comm= 2>/dev/null)
  echo "  占用进程：$PROCESS_INFO (PID: $PID)"
  
  # 直接终止
  kill "$PID" 2>/dev/null
  
  # 等待进程结束（最多 5 秒）
  for i in {1..10}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
      echo "进程 $PID 已终止"
      break
    fi
    sleep 0.5
  done
  
  # 如果进程仍在，强制终止
  if ps -p "$PID" > /dev/null 2>&1; then
    echo "进程未正常退出，强制终止..."
    kill -9 "$PID" 2>/dev/null
    sleep 0.5
  fi
  
  # 清理可能残留的文件描述符
  lsof -ti ":$PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
  echo "端口 $PORT 已释放"
fi

echo "启动图谱查看器，数据目录：$DATA_DIR"
echo "提示：请保持本终端运行，浏览器访问 http://localhost:$PORT"
if [[ -n "$OCR_DIR" ]]; then
  echo "OCR 目录：$OCR_DIR"
  cd graph_viewer && go run . --data "../$DATA_DIR" --ocr "../$OCR_DIR"
else
  cd graph_viewer && go run . --data "../$DATA_DIR"
fi
