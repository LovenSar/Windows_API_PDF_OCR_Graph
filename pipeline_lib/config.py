"""
共享配置 — 所有脚本统一引用的常量、类型白名单、归一化函数

其他模块 (pipeline.py, kg_enrich_v41.py, kg_enrich_v43.py 等)
应当 `from pipeline_lib.config import ...` 而非各自维护副本。
"""

import os
import re

WORKSPACE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(WORKSPACE, "json_output_v4")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "_checkpoint.json")
LLM_CKPT_FILE = os.path.join(OUTPUT_DIR, "_llm_checkpoint.json")
OPS_LOG_FILE = os.path.join(OUTPUT_DIR, "_llm_operations.jsonl")
LLM_CONFIG_FILE = os.path.join(WORKSPACE, "llm_config.json")
MIN_DESC_LENGTH = 8
SCHEMA_VERSION = "windows_api_kg_v4.0"

ALLOWED_ENTITY_TYPES = {
    "function", "structure", "enum", "callback", "macro",
    "constant", "typedef", "union", "interface", "ioctl", "event",
    "method", "property", "notification", "oid", "enum_value",
    "error_code", "parameter", "application", "enum_member",
    "function_pointer", "flags", "structure_member", "field", "message",
    "technology", "attribute", "class", "unknown",
}

_TYPE_SYNONYMS = {
    "struct":      "structure",
    "structur":    "structure",
    "structures":  "structure",
    "flag":        "flags",
    "enumvalue":   "enum_value",
}

TYPE_NODE_KINDS = {
    "structure", "enum", "enum_value", "union",
    "typedef", "constant", "macro", "flags", "error_code",
}

C_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
C_KEYWORDS = {
    "const", "volatile", "signed", "unsigned", "struct", "enum",
    "union", "class", "typedef", "static", "extern", "inline",
    "__in", "__out", "__inout", "_in_", "_out_", "_inout_",
}
POINTER_TRIM_RE = re.compile(r"[\s\*]+")


def normalize_entity_type(et: str) -> str:
    """将 entity_type 归一到白名单集合，异常值标记为 unknown。"""
    if not et:
        return "unknown"
    et = str(et).strip().lower()
    if len(et) > 40:
        return "unknown"
    syn = _TYPE_SYNONYMS.get(et)
    if syn:
        return syn
    if et not in ALLOWED_ENTITY_TYPES:
        return "unknown"
    return et
