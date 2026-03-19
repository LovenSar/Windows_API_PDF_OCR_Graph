#!/usr/bin/env bash
# 图谱可视化页面入口 — 从项目根目录启动 graph_viewer

cd "$(dirname "$0")"
DATA_DIR="${1:-json_output_v4}"
OCR_DIR="${2:-}"

if [[ ! -f "$DATA_DIR/global_entity_index.json" ]]; then
  echo "错误: 未找到 $DATA_DIR/global_entity_index.json"
  echo "请先运行 pipeline 生成数据: python pipeline.py"
  exit 1
fi

echo "启动图谱查看器，数据目录: $DATA_DIR"
if [[ -n "$OCR_DIR" ]]; then
  echo "OCR 目录: $OCR_DIR"
  cd graph_viewer && go run . --data "../$DATA_DIR" --ocr "../$OCR_DIR"
else
  cd graph_viewer && go run . --data "../$DATA_DIR"
fi
