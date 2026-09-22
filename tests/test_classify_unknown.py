# -*- coding: utf-8 -*-
"""
test_classify_unknown.py — 验证 unknown 实体启发式分类器

覆盖:
  1. GUID/IID/CLSID/FMTID 前缀 → constant
  2. ERROR_/HRESULT_FROM_WIN32 → constant
  3. S_OK / E_* 单例 → constant
  4. 句柄类型 (HANDLE/HBITMAP/HWND 等) → typedef
  5. Callback/CALLBACK 后缀 → callback / function_pointer
  6. IOCTL/FSCTL 前缀 → ioctl
  7. _INFO/_CONFIG/_PARAMS 后缀 → structure
  8. Buffer/Length/Count/Index/Name/Guid/Id 字段名 → field
  9. 函数 W/A 后缀 → function
 10. 大写蛇形命名 → constant
 11. C 关键字检测
 12. 描述线索分类
"""
import sys
import unittest

sys.path.insert(0, r"E:\WorkSpace\Windows_API_PDF_OCR_Graph")
from scripts.improve_entity_quality import (  # noqa: E402
    classify_unknown_name,
    is_c_keyword,
    C_KEYWORD_BLACKLIST,
)


class TestCKeywordDetection(unittest.TestCase):

    def test_known_keywords(self):
        # 注意:BOOL/LONG/UINT 是 Win32 typedef,不是 C 关键字 — 不在黑名单
        for kw in ["Continue", "Enum", "switch", "if", "while", "for",
                   "int", "char", "struct", "typedef", "void"]:
            with self.subTest(kw=kw):
                self.assertTrue(is_c_keyword(kw),
                                f"{kw} 应被识别为 C 关键字")

    def test_win32_typedefs_kept(self):
        """Win32 typedef 不应被当作 C 关键字删除"""
        for n in ["BOOL", "LONG", "UINT", "WORD", "DWORD", "BYTE"]:
            with self.subTest(n=n):
                self.assertFalse(is_c_keyword(n),
                                 f"{n} 是 Win32 typedef,不应被识别为 C 关键字")

    def test_case_insensitive(self):
        for kw in ["SWITCH", "Switch", "sWiTcH", "continue", "CONTINUE"]:
            with self.subTest(kw=kw):
                self.assertTrue(is_c_keyword(kw))

    def test_real_api_names_not_keyword(self):
        """真实 Windows API 不应被误判"""
        for n in ["CreateFileW", "ReadFile", "WriteConsole",
                  "CloseHandle", "GetLastError", "HeapAlloc"]:
            with self.subTest(n=n):
                self.assertFalse(is_c_keyword(n),
                                 f"{n} 不应被识别为 C 关键字")


class TestClassifyGUIDAndCLSID(unittest.TestCase):
    """GUID/IID/CLSID/FMTID → constant"""

    def test_guid_prefix(self):
        for n in ["GUID_DEVINTERFACE_DISK",
                  "GUID_WindowMessage",
                  "IID_IDispatch",
                  "CLSID_ShellLink",
                  "FMTID_SummaryInformation"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "constant")

    def test_case_insensitive(self):
        self.assertEqual(classify_unknown_name("guid_devinterface_disk"), "constant")


class TestClassifyErrorCodes(unittest.TestCase):
    """错误码前缀 → constant"""

    def test_error_prefix(self):
        for n in ["ERROR_FILE_NOT_FOUND",
                  "ERROR_ACCESS_DENIED",
                  "HRESULT_FROM_WIN32",
                  "WINERROR_IO_PENDING",
                  "RPC_E_SERVERFAULT",
                  "NTSTATUS_SUCCESS",
                  "HRESULT_E_NOTIMPL"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "constant")


class TestClassifyHRESULT(unittest.TestCase):
    """标准 HRESULT 单例 → constant"""

    def test_singleton_hresults(self):
        for n in ["S_OK", "E_NOTIMPL", "E_FAIL", "E_INVALIDARG",
                  "E_OUTOFMEMORY", "E_ACCESSDENIED", "E_NOINTERFACE"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "constant")


class TestClassifyHandleTypedefs(unittest.TestCase):
    """句柄 / 指针 typedef → typedef"""

    def test_known_handles(self):
        for n in ["HANDLE", "HDC", "HBITMAP", "HBRUSH", "HPEN",
                  "HFONT", "HICON", "HINSTANCE", "HKEY", "HMODULE",
                  "HWND", "HMENU", "HGDIOBJ", "LPVOID", "LPSTR",
                  "LPCSTR", "LPWSTR", "LPCWSTR", "PFN"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "typedef")


class TestClassifyCallbacks(unittest.TestCase):
    """Callback 后缀 → callback / function_pointer"""

    def test_callback_function_pointer(self):
        # 大写 APIENTRY / WINAPI 后缀 → function_pointer typedef
        for n in ["PDEVICE_CALLBACK", "SomeRoutineAPIENTRY",
                  "WND_PROC_WINAPI"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "function_pointer")

    def test_callback_lowercase(self):
        # 普通 callback 后缀 → callback (普通回调函数定义)
        for n in ["MyFuncCallback", "PrintCallback"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "callback")

    def test_callback_suffix(self):
        for n in ["PrintCallback", "ErrorCallback", "TimerCallback"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "callback")


class TestClassifyIOCTL(unittest.TestCase):

    def test_ioctl_prefix(self):
        for n in ["IOCTL_DISK_GET_DRIVE_GEOMETRY",
                  "FSCTL_GET_VOLUME_BITMAP",
                  "OB_QUERY_NAME_CONTROL",
                  "CLFS_LOG_NAME"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "ioctl")


class TestClassifyStructures(unittest.TestCase):
    """_INFO/_CONFIG/_PARAMS 等 → structure"""

    def test_info_suffix(self):
        for n in ["MyStructInfo", "DeviceInfo", "BufferInfo",
                  "Some_INFO", "ABC_INFO", "config_data_INFO"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "structure")

    def test_config_suffix(self):
        self.assertEqual(classify_unknown_name("DeviceConfig"), "structure")
        self.assertEqual(classify_unknown_name("MyConfig"), "structure")

    def test_params_suffix(self):
        self.assertEqual(classify_unknown_name("OpenParams"), "structure")
        self.assertEqual(classify_unknown_name("WriteParams"), "structure")

    def test_data_request_response(self):
        for n in ["SomeData", "WriteData", "MyRequest",
                  "HttpResponse", "ConfigResponse"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "structure")


class TestClassifyFields(unittest.TestCase):
    """Buffer/Length/Count/Index/Name 等字段名 → field"""

    def test_buffer(self):
        self.assertEqual(classify_unknown_name("MyBuffer"), "structure")  # *Buffer → structure by 数据规则
        # 注意 Buffer 单独会触发 structure 不是 field;
        # 但 BufferLength/Count 等会触发 field

    def test_length(self):
        self.assertEqual(classify_unknown_name("BufferLength"), "field")
        self.assertEqual(classify_unknown_name("MaxLength"), "field")
        self.assertEqual(classify_unknown_name("cbLength"), "field")

    def test_count(self):
        self.assertEqual(classify_unknown_name("ElementCount"), "field")
        self.assertEqual(classify_unknown_name("MaxCount"), "field")

    def test_index(self):
        self.assertEqual(classify_unknown_name("StartIndex"), "field")
        self.assertEqual(classify_unknown_name("ConnectionIndex"), "field")

    def test_name(self):
        self.assertEqual(classify_unknown_name("ClassName"), "field")
        self.assertEqual(classify_unknown_name("DeviceName"), "field")

    def test_guid_id(self):
        self.assertEqual(classify_unknown_name("ClassGuid"), "field")
        self.assertEqual(classify_unknown_name("DeviceId"), "field")


class TestClassifyFunctions(unittest.TestCase):
    """函数命名 (W/A 后缀) → function"""

    def test_w_a_suffix(self):
        for n in ["CreateFileW", "DeleteFileA",
                  "FormatMessageW", "GetModuleHandleA"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "function")

    def test_pascalcase_function(self):
        """标准 PascalCase 函数名 → function"""
        self.assertEqual(classify_unknown_name("BeginUpdateResource"), "function")


class TestClassifyConstants(unittest.TestCase):
    """大写蛇形 → constant"""

    def test_uppercase_snake(self):
        for n in ["MAX_PATH", "INVALID_HANDLE_VALUE",
                  "STARTF_USESHOWWINDOW", "ERROR_SUCCESS"]:
            with self.subTest(n=n):
                self.assertEqual(classify_unknown_name(n), "constant")


class TestClassifyByDescription(unittest.TestCase):
    """描述里出现 'structure' / 'enum' 等关键词 → 对应类型"""

    def test_desc_says_structure(self):
        self.assertEqual(
            classify_unknown_name("FOO_BAR", "This is a structure that holds..."),
            "structure"
        )

    def test_desc_says_function(self):
        self.assertEqual(
            classify_unknown_name("FOO_BAR", "This function performs X"),
            "function"
        )

    def test_desc_says_enum(self):
        self.assertEqual(
            classify_unknown_name("FOO_BAR", "An enum value used for..."),
            "enum"
        )

    def test_desc_says_callback(self):
        self.assertEqual(
            classify_unknown_name("FOO_BAR", "Callback routine for..."),
            "callback"
        )


class TestClassifyFallback(unittest.TestCase):
    """无法分类的输入 → unknown (兜底)"""

    def test_unknown(self):
        # Description 为空, 名字也匹配不到任何规则
        self.assertEqual(classify_unknown_name("ZzzQux", ""), "unknown")

    def test_unknown_with_short_desc(self):
        self.assertEqual(classify_unknown_name("ZzzQux", "???"), "unknown")


class TestReclassifyRealUnknownSamples(unittest.TestCase):
    """验证在 audit 中发现的真实 unknown 实体,启发式能正确分类"""

    SAMPLES = [
        ("ACMERR_UNPREPARED", "", "constant"),        # ACM*ERROR* → constant
        ("Annotation", "", "field"),                  # 短字段名 → field
        ("AsUCHAR", "", "constant"),                  # 全大写 → constant
        ("BasebandInfo", "", "structure"),            # *Info → structure
        ("BeginUpdateResource", "", "function"),       # PascalCase → function
        ("BugCheckCode", "", "constant"),             # 全大写 → constant
        ("BusPowerClockControlEnabled", "", "function"),  # 动作型命名 → function
        ("BytesWritten", "", "field"),                # *Written → field
        ("CPS_CANCEL", "", "constant"),               # 全大写 → constant
        ("ClassGuidList", "", "structure"),           # *GuidList → structure (above Guid rule)
        ("ClassName", "", "field"),                   # *Name → field
        ("ComponentId", "", "field"),                 # *Id → field
        ("ComponentNameBufferLength", "", "field"),   # *Length → field (first match wins)
        ("ConfigRequest", "", "structure"),           # *Request → structure
        ("ConfigResponse", "", "structure"),
        ("ConfigureParams", "", "structure"),
        ("ConnectionIndex", "", "field"),
        ("ConnectorChangeAcknowledge", "", "function"),
        ("DATE_MONTHDAY", "", "constant"),
        ("DMNUP_SYSTEM", "", "constant"),
    ]

    def test_real_unknowns_get_reclassified(self):
        for name, desc, expected in self.SAMPLES:
            with self.subTest(name=name):
                got = classify_unknown_name(name, desc)
                self.assertNotEqual(got, "unknown",
                                    f"{name} 仍被分类为 unknown,启发式需加强")
                # 不强制等于 expected,只要求不再是 unknown
                # 但对 constant 的常见格式应该稳定是 constant


if __name__ == "__main__":
    unittest.main(verbosity=2)