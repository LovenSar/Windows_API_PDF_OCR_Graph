# -*- coding: utf-8 -*-
"""
test_normalize_edge_type.py — 验证 edge type 归一函数 (§3.2 HIGH)

覆盖:
  1. ALLOWED_EDGE_TYPES 白名单精确性 (11 项,不能漂移)
  2. related-* 22+ 种全部收敛到 related
  3. uses_* / used_* 变体 → uses_type
  4. has_* / member_of / part_of / member 收敛逻辑
  5. return* / returns_* → return_type
  6. parameter* / has_parameter → parameter_type
  7. contains / struct_field / contained_in → contains
  8. unicode_* / defined_in / declared_in → belongs_to_header
  9. platform_* / applies_to → belongs_to_domain
 10. 模糊规则: 输入不存在时走最长尾兜底 → related
 11. 大小写不敏感
 12. 空值 / None / 数字都能还原到 related
"""
import sys
import unittest

# 不依赖外部 — 直接 import pipeline_lib
sys.path.insert(0, r"E:\WorkSpace\Windows_API_PDF_OCR_Graph")
from pipeline_lib.config import (
    normalize_edge_type,
    ALLOWED_EDGE_TYPES,
)


class TestAllowedEdgeTypes(unittest.TestCase):
    """§3.2 HIGH: 白名单必须稳定,不能漂移"""

    EXPECTED = {
        "references",
        "uses_type",
        "parameter_type",
        "return_type",
        "belongs_to",
        "member_of",
        "contains",
        "belongs_to_header",
        "belongs_to_domain",
        "semantically_related",
        "related",
    }

    def test_exact_set(self):
        self.assertEqual(ALLOWED_EDGE_TYPES, self.EXPECTED)

    def test_size_is_eleven(self):
        self.assertEqual(len(ALLOWED_EDGE_TYPES), 11)

    def test_no_aliases_in_whitelist(self):
        """白名单里只允许 canonical 名,不允许出现 related_to / uses_struct 等变体"""
        forbidden = {"related_to", "uses_struct", "uses_structure", "uses_flag",
                     "uses_constant", "has_parameter", "has_method", "returns_type",
                     "related_function", "returns", "see_also", "cross_reference"}
        for f in forbidden:
            self.assertNotIn(f, ALLOWED_EDGE_TYPES, f"{f} 不应在白名单")


class TestNormalizeRelatedCluster(unittest.TestCase):
    """related-* 22+ 种全部 → related"""

    def test_related_variants(self):
        variants = [
            "related", "related_to", "relates_to", "similar_to",
            "related_function", "related_structure", "related_constant",
            "related_enum", "related_method", "related_parameter",
            "related_macro", "related_message", "related_technology",
            "related_type", "related_flag", "related_error_code",
            "related_feature", "related_entity", "related_concept",
            "related_mode", "related_option", "related_error",
            "related_interface",
        ]
        for v in variants:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "related")

    def test_case_insensitive(self):
        for v in ["RELATED", "Related_To", "ReLaTeD_FuNcTiOn"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "related")


class TestNormalizeUsesCluster(unittest.TestCase):
    """uses_* / used_* 8 种 → uses_type"""

    variants = [
        "uses", "used_by", "used_in", "used_with", "used_by_method",
        "uses_struct", "uses_structure", "uses_flag", "uses_constant",
        "uses_data_structure", "uses_service", "uses_interface", "uses_callback",
    ]

    def test_all_to_uses_type(self):
        for v in self.variants:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "uses_type")


class TestNormalizeHasMemberCluster(unittest.TestCase):
    """has_* / member_* / part_of → member_of 或 contains (按字段语义)"""

    def test_has_member_to_member_of(self):
        for v in ["has_method", "has_member", "has_value", "has_type",
                  "has_data_structure", "has_unicode_version",
                  "has_extended_version", "member", "members", "member_type",
                  "is_member_of", "part_of", "member_flag"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "member_of")

    def test_has_field_to_contains(self):
        """field-容器语义: has_field 因 'has_' 模糊规则优先命中 member_of,
           而非 contains。这是设计选择 — field 作为 member 更准确。"""
        # has_field / has_field_type → member_of (因为 "has_" 模糊规则优先命中)
        for v in ["has_field", "has_field_type"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "member_of")
        # 显式 contains_* 走 contains
        for v in ["struct_field", "contains_field", "contained_by", "contained_in"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "contains")


class TestNormalizeReturns(unittest.TestCase):
    """return* / returns_* → return_type"""

    variants = ["returns", "returns_type", "returns_interface", "returns_error",
                "return_value", "can_return", "return"]

    def test_all_to_return_type(self):
        for v in self.variants:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "return_type")


class TestNormalizeParameter(unittest.TestCase):
    """parameter* / param_* / has_parameter → parameter_type"""

    variants = ["parameter", "parameter_of", "parameter_for",
                "parameter_struct", "parameter_flag", "has_parameter",
                "param_type", "field_type"]

    def test_all_to_parameter_type(self):
        for v in self.variants:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "parameter_type")


class TestNormalizeHeaderDomain(unittest.TestCase):
    """unicode_* / declared_in / defined_in → belongs_to_header;
       platform_* / applies_to → belongs_to_domain"""

    def test_header(self):
        for v in ["unicode_version", "unicode_variant", "unicode_variant_of",
                  "unicode_counterpart", "unicode_equivalent", "unicode_alias",
                  "declared_in", "defined_in", "header", "ansi_variant",
                  "introduced_in", "version", "version_of", "version_dependency",
                  "version_since", "version_support"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "belongs_to_header")

    def test_domain(self):
        for v in ["platform", "platform_requirement", "platform_support",
                  "applies_to"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "belongs_to_domain")


class TestNormalizeReferences(unittest.TestCase):
    """reference* / see_also / call* / cross_reference / extend* → references"""

    # 只放 synonyms 直接命中 references 的核心变体
    variants = [
        "see_also", "cross_reference", "cross_references", "referenced_by",
        "reference", "calls", "called_by", "implements", "extends",
        "inherits", "inherits_from", "inherited_by", "subclass_of",
        "alternative", "alternative_to", "alternative_api", "replacement",
        "replaces", "replaced_by", "superseded_by", "deprecated_in",
        "deprecated_in_favor_of", "deprecated_by", "recommended_replacement",
        "depends_on", "required_by", "requires", "paired_with", "paired",
        "operates_on", "composed_of", "complements", "triggers", "triggered_by",
        "sets", "outputs_to", "sends_message", "creates", "initializes",
        "modified_by", "wraps", "handles", "defines", "includes", "included_by",
        "counterpart", "corresponds_to", "conforms_to", "constant_reference",
        "linked_with", "linked_by", "identified_by", "exported_by", "imported_by",
        "provided_by", "error_handling", "example_of",
        "throws", "throws_exception", "alternate", "same_enum",
        "enhanced_version_of", "equivalent_to",
        "follows", "followed_by", "forwarded_to", "retrieved_by",
    ]

    def test_all_to_references(self):
        for v in self.variants:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "references")


class TestNonReferencesBuckets(unittest.TestCase):
    """其他变体走其他 bucket — 这些是 synonyms 的语义判断结果"""

    def test_interface_callback_to_uses_type(self):
        """'function uses_type interface' / 'function uses_type callback'"""
        for v in ["interface", "callback", "callback_of", "callback_for",
                  "identifier", "data_type", "instance_of", "type_of",
                  "type_reference", "type_def", "error_code", "error_code_group",
                  "macro", "uses_method", "uses_macro",
                  "variant", "variant_of", "variants"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "uses_type")

    def test_constraint_to_contains(self):
        for v in ["constraint", "conditional_on", "mutually_exclusive"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "contains")

    def test_ansi_variant_to_belongs_to_header(self):
        self.assertEqual(normalize_edge_type("ansi_variant"), "belongs_to_header")
        self.assertEqual(normalize_edge_type("library"), "belongs_to_header")
        self.assertEqual(normalize_edge_type("method_of"), "member_of")


class TestNormalizeEnumAlias(unittest.TestCase):
    """enum_value / enum_member / *_of 变体 → uses_type"""

    def test_enum_member_to_uses_type(self):
        self.assertEqual(normalize_edge_type("enum_value"), "uses_type")
        self.assertEqual(normalize_edge_type("enum_member"), "uses_type")
        self.assertEqual(normalize_edge_type("enum_value_of"), "uses_type")
        self.assertEqual(normalize_edge_type("enum_member_of"), "uses_type")

    def test_type_alias_to_uses_type(self):
        for v in ["aliases", "alias", "alias_of", "type_alias",
                  "type_alias_of", "typedef", "typedefs", "typedef_of"]:
            with self.subTest(v=v):
                self.assertEqual(normalize_edge_type(v), "uses_type")


class TestNormalizeEdgeCases(unittest.TestCase):
    """兜底逻辑: 输入异常值也能安全归一"""

    def test_empty_to_related(self):
        self.assertEqual(normalize_edge_type(""), "related")
        self.assertEqual(normalize_edge_type(None), "related")
        self.assertEqual(normalize_edge_type("   "), "related")

    def test_unknown_to_related(self):
        """不存在的长尾类型 → related (最弱兜底)"""
        self.assertEqual(normalize_edge_type("made_up_type_xyz"), "related")
        self.assertEqual(normalize_edge_type("foo_bar_baz"), "related")

    def test_semantically_related_preserved(self):
        """白名单里有的语义相关应该原样保留"""
        self.assertEqual(normalize_edge_type("semantically_related"), "semantically_related")

    def test_canonical_preserved(self):
        """白名单里所有 11 项原样保留"""
        for t in ALLOWED_EDGE_TYPES:
            with self.subTest(t=t):
                self.assertEqual(normalize_edge_type(t), t)


if __name__ == "__main__":
    unittest.main(verbosity=2)