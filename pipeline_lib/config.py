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
    # 历史遗留: §3.4 MEDIUM — enum_member 与 enum_value 是同一语义的两个命名
    # 把 enum_member 收敛到 enum_value,保持 entity type 集合单义
    "enum_member": "enum_value",
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


# ──────────────────────────────────────────────────────────────────────
# Edge type 白名单 + 归一化 (§3.2 HIGH)
# ──────────────────────────────────────────────────────────────────────
#
# 设计目标 (AGENTS.md:71):
#   strong      : references, uses_type, parameter_type, return_type
#   structural  : belongs_to, member_of, contains
#   header/dom  : belongs_to_header, belongs_to_domain
#   weak        : semantically_related
#   兜底        : related (中性关系,无方向)
#
# 历史 global_edges.json 出现 180+ distinct type, 主要是 LLM 在 add_edge 时
# 自由发挥 (related_* / uses_* / has_* 各种变体)。离线 fix 时把所有这些
# 通过 _EDGE_TYPE_SYNONYMS 收敛到 ~12 个核心 + 1 个 related 兜底。

ALLOWED_EDGE_TYPES = {
    # strong — 文本/签名直接引用
    "references",
    "uses_type",
    "parameter_type",
    "return_type",
    # structural — 文档级包含/归属
    "belongs_to",
    "member_of",
    "contains",
    # header/domain — 头文件/领域归属
    "belongs_to_header",
    "belongs_to_domain",
    # weak — 语义/embedding 兜底
    "semantically_related",
    "related",
}

# 边缘 type 同义归一 (lower-case 输入 → 白名单 canonical)
_EDGE_TYPE_SYNONYMS = {
    # related-* 22 种 → related 兜底
    "related":            "related",
    "related_to":         "related",
    "relates_to":         "related",
    "similar_to":         "related",
    "related_function":   "related",
    "related_structure":  "related",
    "related_constant":   "related",
    "related_enum":       "related",
    "related_method":     "related",
    "related_parameter":  "related",
    "related_macro":      "related",
    "related_message":    "related",
    "related_technology": "related",
    "related_type":       "related",
    "related_flag":       "related",
    "related_error_code": "related",
    "related_feature":    "related",
    "related_entity":     "related",
    "related_concept":    "related",
    "related_mode":       "related",
    "related_option":     "related",
    "related_error":      "related",
    "related_interface":  "related",
    # uses_* / used_* 变体 → uses_type
    "uses":               "uses_type",
    "used_by":            "uses_type",
    "used_in":            "uses_type",
    "used_with":          "uses_type",
    "used_by_method":     "uses_type",
    "uses_struct":        "uses_type",
    "uses_structure":     "uses_type",
    "uses_flag":          "uses_type",
    "uses_constant":      "uses_type",
    "uses_data_structure":"uses_type",
    "uses_service":       "uses_type",
    "uses_interface":     "uses_type",
    "uses_callback":      "uses_type",
    # returns_* 变体 → return_type
    "returns":            "return_type",
    "returns_type":       "return_type",
    "returns_interface":  "return_type",
    "returns_error":      "return_type",
    "can_return":         "return_type",
    "return_value":       "return_type",
    # parameter_* / *_parameter / has_parameter 变体 → parameter_type
    "parameter":          "parameter_type",
    "parameter_of":       "parameter_type",
    "parameter_for":      "parameter_type",
    "parameter_struct":   "parameter_type",
    "parameter_flag":     "parameter_type",
    "has_parameter":      "parameter_type",
    # has_* 剩余 → member_of
    "has_method":         "member_of",
    "has_member":         "member_of",
    "has_value":          "member_of",
    "has_field":          "member_of",
    "has_type":           "member_of",
    "has_data_structure": "member_of",
    "has_field_type":     "member_of",
    "has_unicode_version":"member_of",
    "has_extended_version":"member_of",
    "member":             "member_of",
    "members":            "member_of",
    "member_type":        "member_of",
    "memberof":           "member_of",
    "is_member_of":       "member_of",
    "member_flag":        "member_of",
    # 结构成员 / contains 变体 → contains
    "struct_field":       "contains",
    "contains_field":     "contains",
    "contained_by":       "contains",
    "contained_in":       "contains",
    # 类型别名 → type_alias_of 缺省走 uses_type (无独立白名单槽)
    "aliases":            "uses_type",
    "alias":              "uses_type",
    "alias_of":           "uses_type",
    "type_alias":         "uses_type",
    "type_alias_of":      "uses_type",
    "typedef":            "uses_type",
    "typedefs":           "uses_type",
    "typedef_of":         "uses_type",
    # enum value / member 互相纠正 → uses_type (枚举值"使用"了枚举类型)
    "enum_value":         "uses_type",
    "enum_value_of":      "uses_type",
    "enum_member":        "uses_type",
    "enum_member_of":     "uses_type",
    # declared_in / defined_in → belongs_to_header
    "declared_in":        "belongs_to_header",
    "defined_in":         "belongs_to_header",
    "header":             "belongs_to_header",
    # see_also / cross_reference / reference → references
    "see_also":           "references",
    "cross_reference":    "references",
    "cross_references":   "references",
    "referenced_by":      "references",
    "reference":          "references",
    # calls / called_by → references (call 是 reference 的子集)
    "calls":              "references",
    "called_by":          "references",
    # implements / extends / inherits → references
    "implements":         "references",
    "extends":            "references",
    "inherits":           "references",
    "inherits_from":      "references",
    "inherited_by":       "references",
    "subclass_of":        "references",
    # 显式 alternative/replacement 等 → references
    "alternative":        "references",
    "alternative_to":     "references",
    "alternative_api":    "references",
    "replacement":        "references",
    "replaces":           "references",
    "replaced_by":        "references",
    "superseded_by":      "references",
    "deprecated_in":      "references",
    "deprecated_in_favor_of": "references",
    "deprecated_by":      "references",
    "recommended_replacement": "references",
    # depends_on / requires / paired / etc. → references
    "depends_on":         "references",
    "required_by":        "references",
    "requires":           "references",
    "paired_with":        "references",
    "paired":             "references",
    "operates_on":        "references",
    # 复合/接口 → references
    "composed_of":        "references",
    "complements":        "references",
    "paired_with":        "references",
    # version/introduction → belongs_to_header (弱)
    "introduced_in":      "belongs_to_header",
    "version":            "belongs_to_header",
    "version_of":         "belongs_to_header",
    "version_dependency":"belongs_to_header",
    "version_since":     "belongs_to_header",
    "version_support":   "belongs_to_header",
    "unicode_version":    "belongs_to_header",
    "unicode_variant":    "belongs_to_header",
    "unicode_variant_of": "belongs_to_header",
    "unicode_counterpart":"belongs_to_header",
    "unicode_equivalent": "belongs_to_header",
    "unicode_alias":      "belongs_to_header",
    # 平台/系统 → belongs_to_domain
    "platform":           "belongs_to_domain",
    "platform_requirement": "belongs_to_domain",
    "platform_support":   "belongs_to_domain",
    # 其余长尾类型 (按出现频次归类,补到兜底)
    "applies_to":         "belongs_to_domain",
    "variant":            "uses_type",
    "variant_of":         "uses_type",
    "variants":           "uses_type",
    "constraint":         "contains",
    "conditional_on":     "contains",
    "exported_by":        "references",
    "imported_by":        "references",
    "provided_by":        "references",
    "library":            "belongs_to_header",
    "method_of":          "member_of",
    "uses_method":        "uses_type",
    "uses_macro":         "uses_type",
    "related_macro":      "related",  # 已在 related-* 列表
    "interface":          "uses_type",
    "callback":           "uses_type",
    "callback_of":        "uses_type",
    "callback_for":       "uses_type",
    "identifier":         "uses_type",
    "triggers":           "references",
    "triggered_by":        "references",
    "sets":               "references",
    "outputs_to":         "references",
    "sends_message":      "references",
    "creates":            "references",
    "initializes":        "references",
    "modified_by":        "references",
    "wraps":              "references",
    "handles":            "references",
    "defines":            "references",
    "includes":           "references",
    "included_by":        "references",
    "part_of":            "member_of",
    "data_type":          "uses_type",
    "instance_of":        "uses_type",
    "type_of":            "uses_type",
    "type_reference":     "uses_type",
    "type_def":           "uses_type",
    "constraint":         "contains",
    "counterpart":        "references",
    "corresponds_to":     "references",
    "conforms_to":        "references",
    "constant_reference": "references",
    "constant_usage":     "uses_type",
    "has_method":         "member_of",
    "macro":              "uses_type",
    "linked_with":        "references",
    "linked_by":          "references",
    "is_value_of":        "uses_type",
    "identified_by":      "references",
    "flag":               "uses_type",
    "flags":              "uses_type",
    "forwarded_to":       "references",
    "retrieved_by":       "references",
    "follows":            "references",
    "followed_by":        "references",
    "ansi_variant":       "belongs_to_header",
    "alternate":          "references",
    "same_enum":          "references",
    "mutually_exclusive": "contains",
    "enhanced_version_of":"references",
    "equivalent_to":      "references",
    "error_code":         "uses_type",
    "error_code_group":   "uses_type",
    "error_handling":     "references",
    "example_of":         "references",
    "throws":             "references",
    "throws_exception":   "references",
    "dep":                "references",
    "constraint":        "contains",
    "constraint_of":     "contains",
    "field":             "contains",
    "structure_field":   "contains",
    "field_type":        "parameter_type",
    "param_type":        "parameter_type",
    "return":            "return_type",
    "callback_for":      "uses_type",
    "dep":               "references",
    "depends":           "references",
    "doc":               "references",
    "doc_url":           "references",
}


def normalize_edge_type(et: str) -> str:
    """
    将 edge type 归一到白名单集合。

    规则:
      1. 空值 / None / 非字符串 → 'related' (最弱兜底)
      2. strip + lower
      3. 命中 _EDGE_TYPE_SYNONYMS 直接返回 canonical
      4. 命中 ALLOWED_EDGE_TYPES 原样返回
      5. 含 'related' / 'similar' / 'same' 等模糊词 → 'related'
      6. 含 'uses' / 'used' → 'uses_type'
      7. 含 'parameter' / 'param' → 'parameter_type'
      8. 含 'return' → 'return_type'
      9. 含 'header' / 'defined' / 'declared' / 'unicode' → 'belongs_to_header'
      10. 含 'domain' / 'platform' / 'applies' → 'belongs_to_domain'
      11. 含 'member' / 'has_' → 'member_of'
      12. 含 'contain' / 'contains' / 'contained' / 'has_field' → 'contains'
      13. 含 'reference' / 'see' / 'call' / 'cited' / 'related_function' 等 → 'references'
      14. 兜底 → 'related'
    """
    if not et:
        return "related"
    et = str(et).strip().lower()
    if not et:
        return "related"
    syn = _EDGE_TYPE_SYNONYMS.get(et)
    if syn:
        return syn
    if et in ALLOWED_EDGE_TYPES:
        return et
    # 模糊规则 (收尾覆盖 180+ 长尾)
    if any(k in et for k in ("related", "similar", "same_as", "correspond", "equivalent", "counterpart", "paired", "matches")):
        return "related"
    if "uses" in et or "used_" in et:
        return "uses_type"
    if "param" in et:
        return "parameter_type"
    if "return" in et:
        return "return_type"
    if any(k in et for k in ("header", "defined_in", "declared_in", "unicode", "ansi_version")):
        return "belongs_to_header"
    if any(k in et for k in ("domain", "platform", "applies")):
        return "belongs_to_domain"
    if any(k in et for k in ("member", "has_")):
        return "member_of"
    if any(k in et for k in ("contain", "includes", "included", "field", "struct_field")):
        return "contains"
    if any(k in et for k in ("reference", "see_also", "see_", "call", "cite", "cross_ref", "supersede", "replac", "depend", "require", "implement", "extend", "inherit", "alternative", "export", "import", "deprecat", "version_of", "trigger", "create", "initialize", "wrap", "handle", "modify", "send", "output", "follow", "lead", "defines", "linked", "operate")):
        return "references"
    return "related"


# ──────────────────────────────────────────────────────────────────────
# Entity key 归一 (§3.1 HIGH)
# ──────────────────────────────────────────────────────────────────────
#
# 历史 bug: global_entity_index.json 里出现大小写冲突键
#   EvtSerCx2SetWaitMask  vs  EvtSerCx2SetWaitmask
# PowerShell ConvertFrom-Json 拒收,某些下游消费者解析失败。
#
# 规则: 在 Pass-1 输出 + global_index 写入时,把 key 做大小写无关归一,
#       标点/空白去除,统一成 CamelCase / UPPER_SNAKE / lower_snake
#       中最长的版本(尽量保留原意信息)。

import unicodedata as _unicodedata


def normalize_entity_key(key: str) -> str:
    """
    归一 entity key: 保留原大小写风格,只去重。
    - 先去除前后空白
    - 内部连续空白压缩成一个
    - 若整串被 lower 后大小写冲突,优先保留 PascalCase 或 UPPER_SNAKE 中长度更长的那个
      (调用方应在 batch 内先收集所有 keys,统一合并;本函数只做单值清理)
    - 去除零宽字符与 BOM
    """
    if not key:
        return ""
    k = str(key).strip()
    # 去 BOM / 零宽
    k = k.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    # NFKC 归一 (把全角转半角、组合字符拆开)
    k = _unicodedata.normalize("NFKC", k)
    # 压缩连续空白
    k = re.sub(r"\s+", " ", k).strip()
    return k


def merge_entity_keys_case_insensitive(keys_with_data: dict) -> dict:
    """
    给定 {key: entity_dict} 字典,合并大小写冲突项 — 选取数据最完整 (字段非空最多)
    的那个 key 保留,其他 key 的非空字段补到保留项里 (不会覆盖已有字段)。
    返回新的 {key: entity_dict}。
    """
    if not keys_with_data:
        return {}
    # 1. 分组: lowercase -> list of (original_key, data)
    groups: dict = {}
    for k, v in keys_with_data.items():
        nk = normalize_entity_key(k)
        if not nk:
            continue
        lk = nk.lower()
        groups.setdefault(lk, []).append((nk, v))

    merged: dict = {}
    for lk, entries in groups.items():
        if len(entries) == 1:
            merged[entries[0][0]] = entries[0][1]
            continue
        # 多版本: 评分 = (非空字段数, 文本总长)。文本长度用于在字段数相同时
        # 偏好信息更完整的版本 (例如 description 更长的优先保留)。
        def score(item):
            _, data = item
            if not isinstance(data, dict):
                return (0, 0)
            n_fields = 0
            text_len = 0
            for v in data.values():
                if v in (None, "", [], {}):
                    continue
                n_fields += 1
                if isinstance(v, str):
                    text_len += len(v)
                elif isinstance(v, (list, dict)):
                    try:
                        text_len += len(repr(v))
                    except Exception:
                        pass
            return (n_fields, text_len)
        entries.sort(key=score, reverse=True)
        winner_key, winner_data = entries[0]
        winner = dict(winner_data) if isinstance(winner_data, dict) else {}
        # 把后续版本的非空字段补进 winner (不覆盖)
        for k, data in entries[1:]:
            if not isinstance(data, dict):
                continue
            for fk, fv in data.items():
                if fk not in winner or winner[fk] in (None, "", [], {}):
                    if fv not in (None, "", [], {}):
                        winner[fk] = fv
        merged[winner_key] = winner
    return merged
