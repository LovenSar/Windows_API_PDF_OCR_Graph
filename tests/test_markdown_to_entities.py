# -*- coding: utf-8 -*-
"""
test_markdown_to_entities.py — 验证 Markdown 抽取器

覆盖:
  1. YAML front matter 解析 (key-value + 列表)
  2. ## Syntax 段代码块里的函数签名
  3. 参数列表 + SAL 注解清理
  4. ## See also 段链接提取
  5. ## Requirements 段 DLL 提取
  6. entity_type 从 topic_type 映射
  7. A/W 变体 (api_name 多个) 产生 alias entities
  8. 端到端:解析单文件 → entity + edges
"""
import sys
import os
import unittest

sys.path.insert(0, r"E:\WorkSpace\Windows_API_PDF_OCR_Graph")
from scripts.markdown_to_entities import (  # noqa: E402
    parse_front_matter,
    parse_function_signature,
    parse_see_also,
    parse_requirements,
    strip_sal,
    extract_one_doc,
    qualified_name_from_uid,
    _api_name_from_url,
)


# sdk-api 风格：无 ## Syntax，参数在 ## -parameters 的 ### -param x [in] 里，
# 返回类型在 ## -returns 的 Type: 行里
SAMPLE_SDKAPI_MD = """---
UID: NF:winuser.ActivateKeyboardLayout
title: ActivateKeyboardLayout function (winuser.h)
description: Sets the input locale identifier.
req.header: winuser.h
api_name:
 - ActivateKeyboardLayout
---

# ActivateKeyboardLayout function

## -description

Sets the input locale identifier.

## -parameters

### -param hkl [in]

Type: <b>HKL</b>

Input locale identifier to be activated.

### -param Flags [in]

Type: <b>UINT</b>

Specifies how the input locale identifier is to be activated.

## -returns

Type: <b>HKL</b>

The return value is of type HKL.
"""

SAMPLE_SDKAPI_COM_MD = """---
UID: NF:propidl.IPropertySetStorage.Delete
title: IPropertySetStorage::Delete method (propidl.h)
description: Deletes one of the property sets.
req.header: propidl.h
api_name:
 - IPropertySetStorage
---

# IPropertySetStorage::Delete method

## -parameters

### -param rfmtid [in]

Type: <b>REFFMTID</b>

The FMTID of the property set to be deleted.

## -returns

Type: <b>HRESULT</b>

This method supports the standard return values E_UNEXPECTED.
"""


SAMPLE_MD = """---
description: Creates or opens a file or I/O device.
ms.assetid: 9c5c4d3e-7e2d-4d8a-9a8e-2c5f7d8e9f0a
title: 'CreateFile function'
ms.topic: reference
ms.date: 05/31/2018
topic_type:
- APIRef
- kbSyntax
api_name:
- CreateFile
- CreateFileA
- CreateFileW
api_type:
- DllExport
api_location:
- Kernel32.dll
- KernelBase.dll
---

# CreateFile function

Creates or opens a file or I/O device.

## Syntax

```C++
HANDLE CreateFileW(
  LPCWSTR                lpFileName,
  DWORD                  dwDesiredAccess,
  DWORD                  dwShareMode,
  LPSECURITY_ATTRIBUTES  lpSecurityAttributes,
  DWORD                  dwCreationDisposition,
  DWORD                  dwFlagsAndAttributes,
  HANDLE                 hTemplateFile
);
```

## Parameters

`lpFileName` ...

## Return value

If the function succeeds, the return value is an open handle.

## Requirements

| Requirement | Value |
|----------------|--------|
| DLL | Kernel32.dll; KernelBase.dll |

## See also

[CloseHandle](/windows/desktop/api/handleapi/nf-handleapi-closehandle)
[CreateFileMapping](/windows/desktop/api/memoryapi/nf-memoryapi-createfilemappinga)
"""


class TestFrontMatter(unittest.TestCase):

    def test_simple_keyvalue(self):
        text = "---\ntitle: foo\ndate: 2026-01-01\n---\nbody"
        meta, body = parse_front_matter(text)
        self.assertEqual(meta["title"], "foo")
        self.assertEqual(meta["date"], "2026-01-01")
        self.assertEqual(body.strip(), "body")

    def test_list_values(self):
        meta, _ = parse_front_matter(SAMPLE_MD)
        self.assertEqual(meta["description"], "Creates or opens a file or I/O device.")
        self.assertEqual(meta["title"], "'CreateFile function'")
        # 列表字段
        self.assertEqual(meta["topic_type"], ["APIRef", "kbSyntax"])
        self.assertEqual(meta["api_name"], ["CreateFile", "CreateFileA", "CreateFileW"])
        self.assertEqual(meta["api_location"], ["Kernel32.dll", "KernelBase.dll"])

    def test_no_front_matter(self):
        text = "no front matter here"
        meta, body = parse_front_matter(text)
        self.assertEqual(meta, {})
        self.assertEqual(body, text)


class TestSALStripping(unittest.TestCase):

    def test_strip_in(self):
        self.assertEqual(strip_sal("_In_    struct _EXCEPTION_RECORD   *"),
                         "struct _EXCEPTION_RECORD   *")

    def test_strip_inout(self):
        self.assertEqual(strip_sal("_Inout_ struct _CONTEXT *"),
                         "struct _CONTEXT *")

    def test_strip_multiple(self):
        self.assertEqual(strip_sal("_Out_ _In_ int *"),
                         "int *")

    def test_no_sal(self):
        self.assertEqual(strip_sal("int *"), "int *")

    def test_empty(self):
        self.assertEqual(strip_sal(""), "")


class TestFunctionSignature(unittest.TestCase):

    def test_basic_function(self):
        syntax = """```C++
BOOL CreateFileW(
  LPCWSTR lpFileName,
  DWORD dwDesiredAccess
);
```"""
        ret, name, params = parse_function_signature(syntax)
        self.assertEqual(name, "CreateFileW")
        self.assertIn("BOOL", ret)
        self.assertEqual(len(params), 2)
        self.assertEqual(params[0], ("LPCWSTR", "lpFileName"))
        self.assertEqual(params[1], ("DWORD", "dwDesiredAccess"))

    def test_function_with_sal(self):
        syntax = """```C++
BOOL Foo(
  _In_    struct _EXCEPTION_RECORD   *ExceptionRecord,
  _Inout_ struct _CONTEXT            *ContextRecord
);
```"""
        ret, name, params = parse_function_signature(syntax)
        self.assertEqual(name, "Foo")
        # SAL 应该被剥掉
        self.assertEqual(params[0][1], "ExceptionRecord")
        self.assertNotIn("_In_", params[0][0])
        self.assertEqual(params[1][1], "ContextRecord")
        self.assertNotIn("_Inout_", params[1][0])

    def test_void_params(self):
        syntax = """```C++
void Foo(void);
```"""
        ret, name, params = parse_function_signature(syntax)
        self.assertEqual(name, "Foo")
        self.assertEqual(params, [])  # void 被过滤


class TestSeeAlso(unittest.TestCase):

    def test_markdown_links(self):
        text = """
[CloseHandle](/windows/desktop/api/handleapi/nf-handleapi-closehandle)
[CreateFileMapping](/windows/desktop/api/memoryapi/nf-memoryapi-createfilemappinga)
"""
        refs = parse_see_also(text)
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0][0], "CloseHandle")
        self.assertEqual(refs[1][0], "CreateFileMapping")


class TestRequirements(unittest.TestCase):

    def test_dll_extraction(self):
        text = """
| DLL | Msmdun80.dll; Sqlunirl.dll |
"""
        dlls = parse_requirements(text)
        self.assertIn("Msmdun80.dll", dlls)
        self.assertIn("Sqlunirl.dll", dlls)


class TestAPINameFromURL(unittest.TestCase):

    def test_nf_prefix(self):
        url = "/windows/desktop/api/fileapi/nf-fileapi-createfilea"
        self.assertEqual(_api_name_from_url(url), "createfilea")

    def test_ns_prefix(self):
        url = "/windows/desktop/api/winnt/ns-winnt-_osversioninfoa"
        self.assertEqual(_api_name_from_url(url), "_osversioninfoa")

    def test_ne_prefix(self):
        url = "/windows/desktop/api/winnt/ne-winnt-ver_equal"
        self.assertEqual(_api_name_from_url(url), "ver_equal")


class TestEndToEndExtract(unittest.TestCase):
    """解析完整 .md 文档"""

    def setUp(self):
        # 写入临时 .md 文件
        import tempfile
        self.tmpf = tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, encoding="utf-8"
        )
        self.tmpf.write(SAMPLE_MD)
        self.tmpf.close()

    def tearDown(self):
        os.unlink(self.tmpf.name)

    def test_extract_creates_entity(self):
        ents, edges = extract_one_doc(self.tmpf.name)
        self.assertEqual(len(ents), 3)  # CreateFile + A + W
        names = {e["name"] for e in ents}
        self.assertIn("CreateFile", names)
        self.assertIn("CreateFileA", names)
        self.assertIn("CreateFileW", names)

    def test_extract_creates_edges(self):
        ents, edges = extract_one_doc(self.tmpf.name)
        # 应该包含 parameter_type (7 params), return_type (HANDLE), references (CloseHandle + CreateFileMapping)
        types = {e["type"] for e in edges}
        self.assertIn("parameter_type", types)
        self.assertIn("return_type", types)
        self.assertIn("references", types)

    def test_extract_confidence_1(self):
        ents, edges = extract_one_doc(self.tmpf.name)
        for e in ents:
            self.assertEqual(e["confidence"], 1.0,
                             f"{e['name']} 应有 confidence=1.0")

    def test_extract_sal_cleaned(self):
        ents, edges = extract_one_doc(self.tmpf.name)
        # edges 里所有 parameter_type target 不应含 _In_/_Inout_
        for e in edges:
            if e["type"] == "parameter_type":
                self.assertNotIn("_In_", e["target"])
                self.assertNotIn("_Inout_", e["target"])


class TestQualifiedNameFromUid(unittest.TestCase):
    """
    COM 方法页的 api_name 常常只写接口名，按 id 去重时各接口下的同名方法
    会全塌成一个实体。sdk-api 59,420 个 API 页里 29,484 个属这种情况。
    """

    def test_interface_method(self):
        self.assertEqual(
            qualified_name_from_uid("NF:propidl.IPropertySetStorage.Delete"),
            "IPropertySetStorage.Delete")

    def test_member_with_parens(self):
        self.assertEqual(
            qualified_name_from_uid("NF:d3d12.ID3D12Device.GetResourceAllocationInfo"),
            "ID3D12Device.GetResourceAllocationInfo")

    def test_plain_function_has_no_qualifier(self):
        self.assertEqual(qualified_name_from_uid("NF:winuser.ActivateKeyboardLayout"), "")

    def test_empty_and_garbage(self):
        self.assertEqual(qualified_name_from_uid(""), "")
        self.assertEqual(qualified_name_from_uid("not-a-uid"), "")
        self.assertEqual(qualified_name_from_uid(None), "")


class TestSdkApiStylePages(unittest.TestCase):
    """sdk-api 页与 desktop-src 格式不同：没有 ## Syntax，用 ## -returns"""

    def _extract(self, text):
        import tempfile
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8")
        f.write(text)
        f.close()
        try:
            return extract_one_doc(f.name)
        finally:
            os.unlink(f.name)

    def test_param_bracket_annotation_not_swallowed(self):
        # ### -param hkl [in] 的 [in] 标注曾导致正则匹配失败，参数全漏
        ents, edges = self._extract(SAMPLE_SDKAPI_MD)
        params = ents[0]["parameters"]
        self.assertEqual([p["name"] for p in params], ["hkl", "Flags"])
        self.assertEqual([p["type"] for p in params], ["HKL", "UINT"])

    def test_return_type_from_returns_section(self):
        ents, edges = self._extract(SAMPLE_SDKAPI_MD)
        self.assertEqual(ents[0]["return_type"], "HKL")

    def test_param_and_return_edges_created(self):
        ents, edges = self._extract(SAMPLE_SDKAPI_MD)
        by_type = {}
        for e in edges:
            by_type.setdefault(e["type"], set()).add(e["target"])
        self.assertEqual(by_type.get("parameter_type"), {"windows::HKL", "windows::UINT"})
        self.assertEqual(by_type.get("return_type"), {"windows::HKL"})

    def test_uid_qualifies_method_name(self):
        ents, edges = self._extract(SAMPLE_SDKAPI_COM_MD)
        self.assertEqual(ents[0]["name"], "IPropertySetStorage.Delete")


if __name__ == "__main__":
    unittest.main(verbosity=2)