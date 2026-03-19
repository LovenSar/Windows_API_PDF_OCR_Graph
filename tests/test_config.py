"""Tests for pipeline_lib.config — type normalization and constants."""

from pipeline_lib.config import normalize_entity_type, ALLOWED_ENTITY_TYPES, _TYPE_SYNONYMS


class TestNormalizeEntityType:
    def test_empty_returns_unknown(self):
        assert normalize_entity_type("") == "unknown"
        assert normalize_entity_type(None) == "unknown"

    def test_valid_types_pass_through(self):
        for t in ("function", "structure", "enum", "callback", "macro",
                   "constant", "typedef", "union", "interface", "ioctl"):
            assert normalize_entity_type(t) == t

    def test_struct_normalizes_to_structure(self):
        assert normalize_entity_type("struct") == "structure"
        assert normalize_entity_type("Struct") == "structure"
        assert normalize_entity_type("STRUCT") == "structure"

    def test_structur_typo(self):
        assert normalize_entity_type("structur") == "structure"

    def test_structures_plural(self):
        assert normalize_entity_type("structures") == "structure"

    def test_flag_normalizes_to_flags(self):
        assert normalize_entity_type("flag") == "flags"

    def test_enumvalue_variants(self):
        assert normalize_entity_type("enumvalue") == "enum_value"
        assert normalize_entity_type("enum_value") == "enum_value"

    def test_long_string_becomes_unknown(self):
        long_desc = "This is a very long description that was mistakenly used as entity type"
        assert normalize_entity_type(long_desc) == "unknown"

    def test_invalid_type_becomes_unknown(self):
        assert normalize_entity_type("foobar") == "unknown"
        assert normalize_entity_type("123") == "unknown"

    def test_whitespace_stripped(self):
        assert normalize_entity_type("  function  ") == "function"
        assert normalize_entity_type("\tstruct\n") == "structure"

    def test_case_insensitive(self):
        assert normalize_entity_type("Function") == "function"
        assert normalize_entity_type("ENUM") == "enum"

    def test_class_is_allowed(self):
        assert normalize_entity_type("class") == "class"

    def test_all_synonyms_resolve(self):
        for syn, target in _TYPE_SYNONYMS.items():
            assert normalize_entity_type(syn) == target
            assert target in ALLOWED_ENTITY_TYPES
