#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
markdown_to_entities.py — 从 MicrosoftDocs/win32 的 Markdown 抽取 entities/edges

输入: MicrosoftDocs/win32/desktop-src/<topic>/*.md 文件 (含 YAML front matter)
输出: entities + edges JSON,与现有 pipeline.py 兼容的格式

每个 .md 文件 = 一个文档,通常包含 1+ entities (api_name 字段列出)。
YAML front matter 提供结构化元数据:
  - title / description / ms.date / api_name / api_type / api_location
Markdown body 包含:
  - ## Syntax  (C/C++ 代码块签名)
  - ## Parameters  (<dl><dt> 列表)
  - ## Return value
  - ## See also  (cross-references)
  - ## Requirements  (DLL, header)

用法:
  python scripts/markdown_to_entities.py --input-dir <desktop-src-dir> [--topic-dir <subdir>]
  python scripts/markdown_to_entities.py --input-dir <desktop-src-dir> --output <out.json>

Author: v0.2.0 实施
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# ────────────────────────── YAML front matter 解析 ──────────────────────────

def parse_front_matter(text: str) -> tuple:
    """
    解析文档头部的 --- 之间的 YAML。
    返回 (dict_metadata, body_text)。
    YAML 是简化版,只支持:键: 值 / 列表用 - 前缀 / 多行值。
    """
    if not text.startswith("---"):
        return {}, text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}, text
    header = parts[1]
    body = parts[2]
    meta = {}
    current_key = None
    current_list = None
    for line in header.split("\n"):
        stripped = line.rstrip()
        if not stripped:
            continue
        # 列表项:可以是 "  - foo", "\t- foo", "- foo" (YAML 顶层 list)
        if (stripped.startswith("- ") or stripped.startswith("-\t") or
            stripped.startswith("  - ") or stripped.startswith("\t- ") or
            stripped.startswith("    - ")):
            # 列表项
            item = re.sub(r"^[\s\-]+", "", stripped).strip()
            if current_list is not None:
                current_list.append(item)
            continue
        if ":" in stripped:
            key, _, value = stripped.partition(":")
            key = key.strip()
            value = value.strip()
            if value == "":
                # 后面跟着列表
                current_key = key
                current_list = []
                meta[key] = current_list
            else:
                current_key = key
                current_list = None
                meta[key] = value
    return meta, body


# ────────────────────────── Markdown body 解析 ──────────────────────────

# H2 段匹配 (允许 -description / -parameters 等带 - 前缀的 DDI 风格)
SECTION_RE = re.compile(r"^##\s+-?(.+?)\s*$", re.MULTILINE)
# H3 段匹配。DDI 是 "### -param <name>"，sdk-api 是 "### -param hkl [in]"，
# 后面那个 [in]/[out]/[in,out] 标注是可选的，不带会把 sdk-api 的参数全漏掉。
H3_PARAM_RE = re.compile(
    r"^###\s+-param\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\]]*\])?\s*$", re.MULTILINE)
# 代码块 (fenced)
CODE_BLOCK_RE = re.compile(r"```([a-zA-Z+#]*)\n(.*?)```", re.DOTALL)
# 链接 [text](url)
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
# C++ 函数签名解析
FUNC_SIG_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_*\s]*?)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)",
    re.MULTILINE,
)
# C 宏定义: #define NAME(args) expr 或 #define NAME args
MACRO_RE = re.compile(
    r"^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s+(.*)$",
    re.MULTILINE,
)
MACRO_NOARG_RE = re.compile(
    r"^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$",
    re.MULTILINE,
)


def extract_section(body: str, section_name: str) -> str:
    """抽取指定 H2 段的内容 (兼容 -description / -parameters 等带前缀)"""
    # 去掉可能的前缀 -
    sn = section_name.lstrip("-")
    for prefix in ["", "-"]:
        pattern = re.compile(
            rf"^##\s+{re.escape(prefix + sn)}\s*$\n(.*?)(?=^##\s+|\Z)",
            re.MULTILINE | re.DOTALL,
        )
        m = pattern.search(body)
        if m:
            return m.group(1).strip()
    return ""


# "-param xxx [in]" 子段里的类型行，两种写法都有：Type: <b>HKL</b> / Type: HRESULT
TYPE_LINE_RE = re.compile(r"^Type:\s*(?:<b>\s*)?([^<\n]+?)(?:\s*</b>)?\s*$", re.MULTILINE)


def _first_type_in(text: str) -> str:
    m = TYPE_LINE_RE.search(text or "")
    return strip_sal(m.group(1).strip()) if m else ""


def parse_ddi_parameters(body: str) -> list:
    """
    DDI / sdk-api 格式：从 ### -param <name> 子段提取参数。
    每个子段里的 "Type: <b>XXX</b>" 行给出参数类型（之前只取了名字，类型丢了）。
    """
    params = []
    marks = list(H3_PARAM_RE.finditer(body))
    for i, m in enumerate(marks):
        start = m.end()
        end = marks[i + 1].start() if i + 1 < len(marks) else len(body)
        params.append((_first_type_in(body[start:end]), m.group(1)))
    return params


def parse_return_type(body: str) -> str:
    """从 ## -returns / ## Return value 段取返回类型。"""
    for name in ("-returns", "returns", "Return value", "-return-value"):
        sec = extract_section(body, name)
        if sec:
            t = _first_type_in(sec)
            if t:
                return t
    return ""


# SAL 注解前缀 (Microsoft Source-code Annotation Language)
SAL_ANNOTATIONS = {
    "_In_", "_Out_", "_Inout_", "_In_opt_", "_Out_opt_", "_Inout_opt_",
    "_In_z_", "_Out_z_", "_In_reads_", "_Out_writes_", "_In_reads_bytes_",
    "_Out_writes_bytes_", "_In_reads_to_", "_Out_writes_to_",
    "_Field_size_", "_Field_size_opt_", "_Pre_", "_Post_", "_Return_type_",
    "PCSTR", "PCWSTR", "PSTR", "PWSTR", "PVOID", "PCVOID",
}

SAL_RE = re.compile(r"^(_In[_A-Za-z]*|_Out[_A-Za-z]*|_Inout[_A-Za-z]*|_Field[_A-Za-z]*|_Pre[_A-Za-z]*|_Post[_A-Za-z]*|_Return[_A-Za-z]*|_On_failure[_A-Za-z]*)\s+")


def strip_sal(type_str: str) -> str:
    """去掉参数类型开头的 SAL 注解"""
    if not type_str:
        return type_str
    s = type_str
    # 循环去掉前导 SAL tokens (按 token 长度倒序,确保 _In_reads_ 优先于 _In_)
    changed = True
    while changed:
        changed = False
        for tok in sorted(SAL_ANNOTATIONS, key=len, reverse=True):
            if s.startswith(tok):
                after = s[len(tok):]
                # 必须后面接空白或字符串结束
                if not after or after[0].isspace():
                    s = after.lstrip()
                    changed = True
                    break
        # 也去掉 _In_(...) 等带括号的 SAL
        m = re.match(r"^_In_\(\s*[A-Za-z_][A-Za-z0-9_,\s]*\)\s+", s)
        if m:
            s = s[m.end():]
            changed = True
    return s


def parse_function_signature(syntax_text: str) -> tuple:
    """
    解析 C/C++ 代码块里的函数签名 (支持 C 函数 + 宏定义)。
    返回 (return_type, function_name, parameters: list of (type, name))
    """
    code_match = CODE_BLOCK_RE.search(syntax_text)
    if not code_match:
        return "", "", []
    code = code_match.group(2)
    sig = FUNC_SIG_RE.search(code)
    if sig:
        ret_type, func_name, params_str = sig.group(1), sig.group(2), sig.group(3)
        ret_type = strip_sal(ret_type)
        # 解析参数列表
        params = []
        for p in params_str.split(","):
            p = p.strip()
            if not p or p == "..." or p == "void":
                continue
            p = strip_sal(p)
            m = re.match(r"^(.*?)(\*?\s*)([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[\s*\d*\s*\])?$", p)
            if m:
                p_type = (m.group(1) + m.group(2)).strip()
                p_type = strip_sal(p_type)
                p_name = m.group(3)
                params.append((p_type, p_name))
            else:
                params.append((strip_sal(p), ""))
        return ret_type.strip(), func_name, params
    # 试宏定义: #define NAME(args) expr
    macro = MACRO_RE.search(code)
    if macro:
        macro_name, params_str = macro.group(1), macro.group(2)
        params = []
        for p in params_str.split(","):
            p = p.strip()
            if p:
                params.append(("", p))  # 宏参数无类型
        return "macro", macro_name, params
    # 试无参宏: #define NAME value
    macro2 = MACRO_NOARG_RE.search(code)
    if macro2:
        return "macro", macro2.group(1), []
    return "", "", []


def parse_see_also(see_also_text: str) -> list:
    """抽取 See also 段里的所有链接目标 (Windows API name)"""
    refs = []
    for m in LINK_RE.finditer(see_also_text):
        text, url = m.group(1), m.group(2)
        # MS Docs URL 末段常是 nf-<api> 等,从 URL 提取 API 引用名
        # 也用 markdown 链接文字作为 fallback
        if "/windows/desktop/api/" in url or "/windows-hardware/drivers/" in url:
            refs.append((text, url))
        else:
            refs.append((text, url))
    return refs


def parse_requirements(req_text: str) -> list:
    """从 Requirements 表抽出 DLL 名"""
    dlls = []
    for line in req_text.split("\n"):
        if "DLL" in line.upper() or ".dll" in line.lower():
            # 提取 .dll 名字
            for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*\.dll)", line):
                dlls.append(m.group(1))
    return list(set(dlls))


# ────────────────────────── 主抽取器 ──────────────────────────

# API 类型映射 (YAML topic_type → entity_type)
TOPIC_TYPE_TO_ENTITY_TYPE = {
    "APIRef": "function",
    "apiref": "function",  # DDI 用小写
    "kbSyntax": "function",
    "COMMethod": "method",
    "COMProperty": "property",
    "COMEvent": "event",
    "Struct": "structure",
    "Union": "union",
    "Enum": "enum",
    "Constant": "constant",
    "Macro": "macro",
    "DllImport": "function",
    "DllExport": "function",
    "HeaderDef": "macro",  # DDI 风格
    "DeviceFunction": "function",
    "Interface": "interface",
}

# DDI 风格 api_type 映射
DDI_API_TYPE_TO_ENTITY = {
    "HeaderDef": "macro",
    "DllExport": "function",
    "DeviceFunction": "function",
    "COMMethod": "method",
    "COMProperty": "property",
    "COMEvent": "event",
    "Interface": "interface",
}


# UID 形如 NF:propidl.IPropertySetStorage.Delete（kind:header.Interface.Member）
UID_RE = re.compile(r"^([A-Za-z]{2}):([^.]+)\.([^.]+)\.(.+)$")


def qualified_name_from_uid(uid: str) -> str:
    """UID 带接口限定时返回 Interface.Member，否则返回空串。"""
    if not uid:
        return ""
    m = UID_RE.match(str(uid).strip())
    if not m:
        return ""
    return "%s.%s" % (m.group(3), m.group(4))


def extract_one_doc(md_path: str) -> tuple:
    """
    抽取单个 .md 文件 → (entities_list, edges_list)
    兼容 desktop-src (Win32) 和 wdk-ddi-src (Driver DDI) 两种 Markdown 风格。
    """
    with open(md_path, encoding="utf-8") as f:
        text = f.read()

    meta, body = parse_front_matter(text)

    # 1. 主 entity (api_name 列表的第 1 个)
    api_names = meta.get("api_name", [])
    if isinstance(api_names, str):
        api_names = [api_names]
    if not api_names:
        title = meta.get("title", "")
        m = re.search(r"(_?\w+)", title)
        api_names = [m.group(1)] if m else [os.path.splitext(os.path.basename(md_path))[0].lstrip("-")]

    primary_name = api_names[0]

    # 1b. COM 方法页的 api_name 常常只写接口名（uid 才是全的）。
    #     UID 形如 NF:propidl.IPropertySetStorage.Delete，去掉头文件段即限定名。
    #     不取限定名的话，各接口下的同名方法（Delete / Copy / GetCount …）
    #     会在按 id 去重时全部塌成一个实体 —— sdk-api 有 29,484 页属这种情况。
    qualified = qualified_name_from_uid(meta.get("UID", ""))
    if qualified:
        primary_name = qualified
        api_names = [qualified]

    # 2. 决定 entity_type — 优先 topic_type,然后 api_type
    topic_types = meta.get("topic_type", [])
    if isinstance(topic_types, str):
        topic_types = [topic_types]
    entity_type = "function"  # 默认
    for t in topic_types:
        if t in TOPIC_TYPE_TO_ENTITY_TYPE:
            entity_type = TOPIC_TYPE_TO_ENTITY_TYPE[t]
            break
    # DDI: api_type 也提供类型线索 (HeaderDef → macro 等)
    api_types = meta.get("api_type", [])
    if isinstance(api_types, str):
        api_types = [api_types]
    for at in api_types:
        if at in DDI_API_TYPE_TO_ENTITY:
            entity_type = DDI_API_TYPE_TO_ENTITY[at]
            break

    # 3. 抽取 Syntax 段里的签名 (desktop-src 风格)
    syntax_text = extract_section(body, "Syntax")
    ret_type, func_name, params = parse_function_signature(syntax_text)

    # 3b. DDI / sdk-api 风格:### -param 段
    if not params:
        ddi_params = parse_ddi_parameters(extract_section(body, "-parameters") or extract_section(body, "Parameters"))
        params = ddi_params
    # 3c. sdk-api 页没有 ## Syntax，返回类型在 ## -returns 的 Type: 行
    if not ret_type:
        ret_type = parse_return_type(body)

    # 4. 抽取 See also (兼容 -see-also)
    see_also_text = (extract_section(body, "See also")
                     or extract_section(body, "-see-also")
                     or extract_section(body, "Related topics"))
    see_also = parse_see_also(see_also_text)

    # 5. 抽取 header / DLL
    dlls = parse_requirements(extract_section(body, "Requirements"))
    # DDI 风格:直接用 req.header YAML 字段
    req_header = meta.get("req.header", "")
    if isinstance(req_header, str) and req_header:
        dlls = [req_header]  # 头文件
    # api_location 是另一个线索
    api_loc = meta.get("api_location", [])
    if isinstance(api_loc, str):
        api_loc = [api_loc]
    for loc in api_loc:
        if loc and loc not in dlls:
            dlls.append(loc)

    # 6. 主 entity
    primary_entity = {
        "id": f"windows::{primary_name}",
        "name": primary_name,
        "entity_type": entity_type,
        "description": meta.get("description", ""),
        "header": dlls[0] if dlls else "",
        "ms_date": meta.get("ms.date", ""),
        "uid": meta.get("UID", ""),
        "tech_root": meta.get("tech.root", ""),
        "api_type": meta.get("api_type", []),
        "api_location": dlls,
        "return_type": ret_type,
        "parameters": [{"name": pname, "type": ptype} for ptype, pname in params],
        "source_file": md_path,
        "source_format": "markdown",
        "confidence": 1.0,
    }

    entities = [primary_entity]

    # 7. 处理 api_name 列表里其余名字 (A/W 变体)
    for name in api_names[1:]:
        entities.append({
            "id": f"windows::{name}",
            "name": name,
            "entity_type": entity_type,
            "description": meta.get("description", ""),
            "alias_of": primary_name,
            "source_file": md_path,
            "source_format": "markdown",
            "confidence": 1.0,
        })

    # 8. 构造 edges
    edges = []
    pid = primary_entity["id"]
    if ret_type and ret_type not in ("macro", ""):
        edges.append({"source": pid, "target": f"windows::{ret_type}", "type": "return_type"})
    for ptype, pname in params:
        if ptype:
            edges.append({"source": pid, "target": f"windows::{ptype}", "type": "parameter_type"})
    for text, url in see_also:
        ref_name = _api_name_from_url(url) or text
        if ref_name and not ref_name.startswith("http") and not ref_name.startswith("./"):
            edges.append({"source": pid, "target": f"windows::{ref_name}", "type": "references"})
        elif ref_name and ref_name.startswith("./"):
            # DDI 风格: ./nf-xxx-yyy.md → yyy
            base = os.path.basename(ref_name).replace(".md", "")
            if base.startswith("nf-"):
                parts = base.split("-")
                if len(parts) > 1:
                    ref_name = parts[-1]
                    edges.append({"source": pid, "target": f"windows::{ref_name}", "type": "references"})
    if dlls:
        for d in dlls:
            edges.append({"source": pid, "target": f"windows::{d}", "type": "belongs_to_header"})

    return entities, edges


def _api_name_from_url(url: str) -> str:
    """从 MS Docs URL 提取 API 名 (例: /windows/desktop/api/fileapi/nf-fileapi-createfilea → createfilea)"""
    if not url:
        return ""
    # 既支持绝对 URL 也支持相对路径
    if url.startswith("http") or url.startswith("/"):
        last = url.rstrip("/").split("/")[-1]
        last = last.split(".")[0]
        if last.startswith(("nf-", "ns-", "ne-")):
            parts = last.split("-")
            if len(parts) > 1:
                return parts[-1]
        return last
    return ""


# ────────────────────────── 主流程 ──────────────────────────

def process_directory(input_dir: str, topic: str = None) -> tuple:
    """处理一个目录,返回 (entities, edges)"""
    all_entities = {}
    all_edges = []
    md_files = []
    for root, dirs, files in os.walk(input_dir):
        for fn in files:
            if fn.endswith(".md"):
                md_files.append(os.path.join(root, fn))

    for md_path in md_files:
        try:
            ents, edges = extract_one_doc(md_path)
            for e in ents:
                key = e["id"]
                if key not in all_entities:
                    all_entities[key] = e
            all_edges.extend(edges)
        except Exception as ex:
            print(f"WARN {md_path}: {ex}", file=sys.stderr)
    return all_entities, all_edges


def main():
    ap = argparse.ArgumentParser(description="Markdown → entities/edges 抽取器")
    ap.add_argument("--input-dir", required=True,
                    help="MicrosoftDocs/win32 desktop-src/<topic> 或整个 desktop-src")
    ap.add_argument("--topic", default=None,
                    help="可选主题标识 (写入 entity.api_topic)")
    ap.add_argument("--output", default=None,
                    help="输出 JSON 路径 (默认 stdout)")
    args = ap.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f"ERROR: 输入目录不存在: {args.input_dir}", file=sys.stderr)
        return 2

    ents, edges = process_directory(args.input_dir, args.topic)

    # 加上 topic 标记
    if args.topic:
        for k in ents:
            ents[k]["api_topic"] = args.topic

    out = {
        "_schema": "extracted_v0.2.0",
        "_source": "MicrosoftDocs/win32 (Markdown)",
        "_generated_at": datetime.now().isoformat(),
        "entities": ents,
        "edges": edges,
    }

    output = json.dumps(out, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"已写入: {args.output}")
        print(f"  entities: {len(ents)}")
        print(f"  edges:    {len(edges)}")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())