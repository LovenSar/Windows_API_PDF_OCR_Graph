# -*- coding: utf-8 -*-
"""
test_normalize_entity_key.py — 验证 entity key 归一函数 (§3.1 HIGH)

覆盖:
  1. 大小写无关合并 (EvtSerCx2SetWaitMask vs EvtSerCx2SetWaitmask)
  2. 评分函数: 字段数相同时取文本总长更长的
  3. 非空字段补到 winner,不覆盖已有
  4. 单版本原样保留
  5. NFKC 归一 (全角转半角)
  6. 去 BOM / 零宽字符
  7. 空白压缩
  8. 防御 None / 空值
"""
import sys
import unittest

sys.path.insert(0, r"E:\WorkSpace\Windows_API_PDF_OCR_Graph")
from pipeline_lib.config import (
    normalize_entity_key,
    merge_entity_keys_case_insensitive,
)


class TestNormalizeEntityKey(unittest.TestCase):

    def test_strip_whitespace(self):
        self.assertEqual(normalize_entity_key("  hello  "), "hello")
        self.assertEqual(normalize_entity_key("\t\nabc\n"), "abc")

    def test_compress_internal_whitespace(self):
        self.assertEqual(normalize_entity_key("Hello   World"), "Hello World")
        self.assertEqual(normalize_entity_key("a\t\t\tb"), "a b")

    def test_remove_bom(self):
        self.assertEqual(normalize_entity_key("\ufeffTest"), "Test")

    def test_remove_zero_width(self):
        self.assertEqual(normalize_entity_key("a\u200bb\u200cc"), "abc")
        self.assertEqual(normalize_entity_key("a\u200db"), "ab")

    def test_nfkc_fullwidth_to_ascii(self):
        """全角 'ＡＢＣ' → 半角 'ABC'"""
        self.assertEqual(normalize_entity_key("\uff21\uff22\uff23"), "ABC")

    def test_idempotent(self):
        """归一后再归一应不变"""
        for s in ["EvtSerCx2SetWaitMask", "Hello World", "BOM\ufeffTest", "  trim me  "]:
            once = normalize_entity_key(s)
            twice = normalize_entity_key(once)
            self.assertEqual(once, twice, f"idempotent failed for {s!r}")

    def test_empty_returns_empty(self):
        self.assertEqual(normalize_entity_key(""), "")
        self.assertEqual(normalize_entity_key(None), "")
        self.assertEqual(normalize_entity_key("   "), "")


class TestMergeEntityKeysCaseInsensitive(unittest.TestCase):
    """§3.1 HIGH bug 修复: PowerShell 拒收大小写冲突键"""

    def test_canonical_collision_merged(self):
        """已知 bug 案例"""
        data = {
            "EvtSerCx2SetWaitMask": {
                "name": "EvtSerCx2SetWaitMask",
                "type": "function",
                "description": "W version short",
            },
            "EvtSerCx2SetWaitmask": {
                "name": "EvtSerCx2SetWaitmask",
                "type": "function",
                "description": "w version longer description",
            },
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(len(merged), 1)
        # 长度优先 → 取长 description 的版本
        winner = list(merged.values())[0]
        self.assertIn("longer description", winner["description"])

    def test_field_count_tiebreak(self):
        """字段多的优先"""
        data = {
            "AbcDef": {"name": "AbcDef", "type": "function"},
            "Abcdef": {"name": "Abcdef", "type": "function", "description": "more info"},
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(len(merged), 1)
        winner = list(merged.values())[0]
        # winner 应包含 description
        self.assertIn("description", winner)
        self.assertEqual(winner["description"], "more info")

    def test_non_overlap_merge(self):
        """非空字段不覆盖:loser 的独有字段补进 winner"""
        data = {
            "FooBar": {"name": "FooBar", "type": "function"},
            "Foobar": {"name": "Foobar", "type": "function", "header": "foo.h", "url": "x"},
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(len(merged), 1)
        winner = list(merged.values())[0]
        self.assertEqual(winner["type"], "function")
        # 字段多的 Foobar 应该胜出 (header + url 是 Foobar 独有)
        self.assertIn("header", winner)
        self.assertIn("url", winner)

    def test_no_collision_passthrough(self):
        """无冲突的字典原样保留"""
        data = {
            "CreateFileW": {"name": "CreateFileW", "type": "function"},
            "ReadFile": {"name": "ReadFile", "type": "function"},
            "CloseHandle": {"name": "CloseHandle", "type": "function"},
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(len(merged), 3)
        self.assertEqual(set(merged.keys()), {"CreateFileW", "ReadFile", "CloseHandle"})

    def test_empty_input(self):
        self.assertEqual(merge_entity_keys_case_insensitive({}), {})
        self.assertEqual(merge_entity_keys_case_insensitive(None), {})

    def test_empty_key_filtered(self):
        """空字符串 key 应该被过滤掉"""
        data = {
            "": {"name": "empty"},
            "ValidKey": {"name": "ValidKey"},
            "   ": {"name": "spaces"},
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(list(merged.keys()), ["ValidKey"])

    def test_three_way_collision(self):
        """三路大小写冲突合并"""
        data = {
            "abcdef": {"name": "abcdef"},
            "Abcdef": {"name": "Abcdef", "type": "function"},
            "ABCDEF": {"name": "ABCDEF", "type": "function", "header": "abc.h"},
        }
        merged = merge_entity_keys_case_insensitive(data)
        self.assertEqual(len(merged), 1)
        winner = list(merged.values())[0]
        # 字段最多的是 ABCDEF (3 fields vs 2 vs 1) → 胜
        self.assertIn("header", winner)
        self.assertEqual(winner["header"], "abc.h")


if __name__ == "__main__":
    unittest.main(verbosity=2)