#!/usr/bin/env python3
"""Fail-closed unit tests for WinAPI agents_doccheck (same family as Hub/satellites)."""

from __future__ import annotations

import unittest
from pathlib import Path

import check

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "testdata"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FactFixtureTests(unittest.TestCase):
    def test_missing_owner_fail_closed(self) -> None:
        text = _read("facts_missing_owner.md")
        facts = check.parse_facts(text)
        errors = check.fact_field_errors("testdata/facts_missing_owner.md", facts)
        self.assertTrue(
            any("missing field owner" in item for item in errors),
            errors,
        )

    def test_complete_facts_have_no_field_errors(self) -> None:
        text = _read("facts_ok.md")
        facts = check.parse_facts(text)
        errors = check.fact_field_errors("testdata/facts_ok.md", facts)
        self.assertEqual(errors, [])


class VersionPolicyTests(unittest.TestCase):
    def test_interface_removed_with_patch_fail_closed(self) -> None:
        old = _read("facts_ok.md")
        new = _read("facts_interface_removed_patch.md")
        errors = check.version_policy_errors("AGENTS.md", old, new)
        self.assertTrue(
            any("removed" in item and "MAJOR" in item for item in errors),
            errors,
        )


class ReadmeMatrixTests(unittest.TestCase):
    def test_matrix_drift_fail_closed(self) -> None:
        by_id = {
            "docs": {"dir": "docs", "facts": {"internal_version": "0.1.0"}},
            "tools": {"dir": "tools", "facts": {"internal_version": "0.1.0"}},
        }
        errors = check.readme_matrix_errors(_read("readme_matrix_drift.md"), by_id)
        self.assertTrue(any("component_id set mismatch" in item for item in errors), errors)

    def test_matching_matrix_has_no_set_error(self) -> None:
        by_id = {
            "docs": {"dir": "docs", "facts": {"internal_version": "0.1.0"}},
            "tools": {"dir": "tools", "facts": {"internal_version": "0.1.0"}},
        }
        errors = check.readme_matrix_errors(_read("readme_matrix_ok.md"), by_id)
        mismatch = [item for item in errors if "component_id set mismatch" in item]
        self.assertEqual(mismatch, [])


if __name__ == "__main__":
    unittest.main()
