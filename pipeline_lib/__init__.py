"""
Windows API 知识图谱流水线 — 模块化包

从 pipeline.py 的 2000+ 行单体中拆分出的可复用模块：
  config    — 常量、白名单、类型归一化
  parsers   — OCR 纠错、子段落解析、requirements 解析
  llm       — 异步 LLM 客户端、速率控制
  graph     — KnowledgeGraph 结构 + OperationExecutor
"""

from pipeline_lib.config import (
    WORKSPACE, OUTPUT_DIR, SCHEMA_VERSION,
    ALLOWED_ENTITY_TYPES, TYPE_NODE_KINDS,
    normalize_entity_type,
)
