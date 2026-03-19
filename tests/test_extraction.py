"""Tests for extraction utilities — OCR fix, parsing, syntax analysis."""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import (
    ocr_fix_entity_name, is_noise, parse_parameters, parse_requirements,
    parse_return_value, infer_entity_type,
)
from enrich_from_syntax import parse_signature, extract_header_from_requirements, extract_domain_from_filename


class TestOCRFix:
    def test_common_ocr_errors(self):
        assert ocr_fix_entity_name("UL0NG") == "ULONG"
        assert ocr_fix_entity_name("B00L") == "BOOL"
        assert ocr_fix_entity_name("DW0RD") == "DWORD"
        assert ocr_fix_entity_name("HANDlE") == "HANDLE"
        assert ocr_fix_entity_name("NUlL") == "NULL"

    def test_clean_name_unchanged(self):
        assert ocr_fix_entity_name("CreateFileW") == "CreateFileW"
        assert ocr_fix_entity_name("OVERLAPPED") == "OVERLAPPED"

    def test_hyphen_to_underscore(self):
        assert ocr_fix_entity_name("FILE-FLOPPY-DISKETTE") == "FILE_FLOPPY_DISKETTE"


class TestNoise:
    def test_known_noise(self):
        assert is_noise("此页面是否有帮助？")
        assert is_noise("反馈")

    def test_real_content_not_noise(self):
        assert not is_noise("HANDLE CreateFile(LPCWSTR lpFileName)")
        assert not is_noise("Returns HRESULT on success")


class TestParseParameters:
    def test_direction_and_name(self):
        lines = ["[in] DeviceObject", "Pointer to the device object."]
        params = parse_parameters(lines)
        assert len(params) == 1
        assert params[0]["name"] == "DeviceObject"

    def test_plain_name(self):
        lines = ["hWnd", "A handle to the window."]
        params = parse_parameters(lines)
        assert len(params) == 1
        assert params[0]["name"] == "hWnd"

    def test_type_from_prefix_line(self):
        lines = ["[in] dwFlags", "类型：DWORD", "Flags value."]
        params = parse_parameters(lines)
        assert len(params) == 1
        assert params[0].get("type") == "DWORD"

    def test_empty_input(self):
        assert parse_parameters([]) == []


class TestParseRequirements:
    def test_header_extraction(self):
        lines = ["标头 winuser.h", "库 User32.lib", "DLL User32.dll"]
        req = parse_requirements(lines)
        assert req.get("header") == "winuser.h"
        assert req.get("library") == "User32.lib"

    def test_empty(self):
        assert parse_requirements([]) == {}


class TestParseSyntax:
    def test_simple_function(self):
        syn = "HRESULT CreateFile(LPCWSTR lpFileName, DWORD dwAccess);"
        result = parse_signature(syn)
        assert result is not None
        ret, name, params = result
        assert ret == "HRESULT"
        assert name == "CreateFile"
        assert len(params) == 2
        assert params[0] == ("LPCWSTR", "lpFileName")
        assert params[1] == ("DWORD", "dwAccess")

    def test_with_sal_annotations(self):
        syn = "NTSTATUS Func([in] PDEVICE_OBJECT Dev, [out] PULONG Size);"
        result = parse_signature(syn)
        assert result is not None
        _, name, params = result
        assert name == "Func"
        assert len(params) == 2
        assert params[0][1] == "Dev"
        assert params[1][1] == "Size"

    def test_void_return(self):
        syn = "void FreeMemory(PVOID ptr);"
        result = parse_signature(syn)
        assert result is not None
        ret, _, _ = result
        assert ret == "void"

    def test_multiline(self):
        syn = "NTSTATUS\\nMyFunc(\\n  ULONG Param1,\\n  PVOID Param2\\n);"
        result = parse_signature(syn)
        assert result is not None
        _, name, params = result
        assert name == "MyFunc"
        assert len(params) == 2

    def test_no_params(self):
        syn = "DWORD GetVersion(void);"
        result = parse_signature(syn)
        assert result is not None
        _, name, params = result
        assert name == "GetVersion"
        assert params == []

    def test_invalid_syntax_returns_none(self):
        assert parse_signature("not a function") is None
        assert parse_signature("") is None
        assert parse_signature(None) is None


class TestExtractHeader:
    def test_dict_with_header(self):
        assert extract_header_from_requirements({"header": "winuser.h"}) == "winuser.h"

    def test_dict_without_header(self):
        assert extract_header_from_requirements({"library": "foo.lib"}) is None

    def test_string(self):
        assert extract_header_from_requirements("Header: winuser.h") == "winuser.h"

    def test_none(self):
        assert extract_header_from_requirements(None) is None


class TestExtractDomain:
    def test_hardware_driver(self):
        assert extract_domain_from_filename("hardware-drivers-ddi-_acpi_20260305_0120.json") == "acpi"

    def test_win32_api(self):
        assert extract_domain_from_filename("win32-api-_gdi_20260305_0148.json") == "gdi"

    def test_unrecognized(self):
        assert extract_domain_from_filename("random_file.json") is None


class TestInferEntityType:
    def test_ioctl_pattern(self):
        assert infer_entity_type("IOCTL_STORAGE_GET_INFO", "unknown") == "ioctl"

    def test_function_pattern(self):
        assert infer_entity_type("CreateFileW", "unknown") == "function"

    def test_no_match_returns_current(self):
        assert infer_entity_type("SomeRandomName", "constant") == "constant"

    def test_empty_current_type_returns_empty(self):
        assert infer_entity_type("SomeName", "") == ""
