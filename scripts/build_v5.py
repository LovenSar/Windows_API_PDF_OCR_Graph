#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_v5.py — 把两个来源合并成 v5 并集库

背景
----
v5 之前的两份产物覆盖的其实是两批不相交的 API：

  json_output_v4/                 OCR/PDF 源，Win32 桌面 API 为主，18,534 实体
  json_output_v4_v020_markdown/   Markdown 源，Driver DDI 为主，23,261 实体

两者同名交集只有 2,605（11%）。本地 desktop-src 只有 124 个 nf- 文件，
Win32 桌面 API 在本地 markdown 里根本不存在，所以这不是「谁更干净」的问题，
是并集问题。

v5 的做法
---------
markdown 侧：
  * entity_type 按文件名前缀重定（nf/ns/nc/ne/ni/nn/nl/wm/mm/nt），
    修掉 markdown_to_entities.py:302 把全部实体压成 function 的默认值
  * 剔除门户页 / 汇总页 / 头文件说明页 / C 关键字
OCR 侧：
  * 只做能双向确认的 3 条名字修正，其余一律不改名（宁可留重，不做编辑距离猜测）
  * 独有的 15,929 个实体全量并入，走 provenance 标记而不是编造 confidence

输出
----
  json_output_v5/global_entity_index.json
  json_output_v5/global_edges.json
  json_output_v5/_v5_build_report.json

用法
----
  python scripts/build_v5.py
  python scripts/build_v5.py --ocr-dir json_output_v4 \
      --md-dir json_output_v4_v020_markdown --output-dir json_output_v5
"""
import argparse
import collections
import json
import os
import re
import sys

# 三个上游源的默认位置。sdk-api / DDI 是外部 clone，不在本仓内，所以走绝对路径。
SDKAPI_DEFAULT = r"D:\KnowLedgeBase\_raw\Tools\MS_Docs_SdkApi\sdkapi_entities.json"
SDKAPI_SRC_DEFAULT = r"D:\KnowLedgeBase\_raw\Tools\MS_Docs_SdkApi\sdk-api\sdk-api-src\content"
DDI_SRC_DEFAULT = (r"D:\KnowLedgeBase\_raw\Tools\MS_Docs_DriverDDI"
                   r"\windows-driver-docs-ddi-staging\wdk-ddi-src\content")

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pipeline_lib.config import normalize_entity_type, normalize_edge_type  # noqa: E402


# ────────────────────────── 文件名前缀 → entity_type ──────────────────────────
#
# 这张表是读样本文件确认过的，不是按 MS Docs 命名习惯猜的。实测反例：
#   ni- 不是 interface，是 IOCTL（IOCTL_NFCSERM_QUERY_RADIO_STATE 等 800 个）
#   nn- 才是 interface（IDebugAdvanced3、IPrintClassObjectFactory）
#   nl- 是 C++ class（CWiauFormatConverter）
#   mf-/em-/ec-/tb-/nm-/to-/cb-/ip-/rc-/cd-/vs-/dx- 不是类型前缀，
#     ip-address-controls.md / rc-diagnostic-messages.md 这类会误命中，
#     所以不在表里的前缀一律不认。

PREFIX_TO_ENTITY_TYPE = {
    "nf": "function",
    "ns": "structure",
    "nc": "callback",
    "ne": "enum",
    "ni": "ioctl",
    "nn": "interface",
    "nl": "class",
    "wm": "message",
    "mm": "message",
    "nt": "function",
}

# 描述形态兜底（MS 文档措辞高度公式化）。顺序敏感：callback 必须排在 function 前，
# 因为 "callback function" 同时命中两个词。
DESC_SHAPE_RULES = [
    (re.compile(r"\benumeration\b", re.I), "enum"),
    (re.compile(r"\bcallback (function|routine)\b", re.I), "callback"),
    (re.compile(r"\bstructure\b", re.I), "structure"),
    (re.compile(r"\bunion\b", re.I), "union"),
    (re.compile(r"\binterface\b", re.I), "interface"),
    (re.compile(r"\bmacro\b", re.I), "macro"),
    (re.compile(r"\bflag", re.I), "flags"),
    (re.compile(r"\bfunction\b|\broutine\b", re.I), "function"),
]

# ────────────────────────── 垃圾页判据 ──────────────────────────
#
# 只用能确证的强判据。"有 ## Syntax 就保留" 这条不用 —— 实测无前缀文件里
# 只有 16.4% 有该标记，DDI 大量页面走 ## -description 风格，用它会误杀真 API。

DROP_DESC_RE = re.compile(r'^"?\s*Learn more about\s*:', re.I)

DROP_BASENAMES = {
    "index.md",              # DDI 目录索引，api_name 里塞的是 "1394" 这种东西
    "overview.md",
    "api-reference.md",
    "functions.md",
    "structures.md",
    "enumerations.md",
    "interfaces.md",
    "methods.md",
    "constants.md",
    "callbacks.md",
    "api-and-terminology.md",
}

DROP_BASENAME_RE = re.compile(r"(-portal|-api-overview|api-and-terminology)\.md$", re.I)

# MCI 命令 / C 关键字，不是 API
DROP_NAMES = {
    "if", "else", "elif", "endif", "ifdef", "ifndef", "define", "undef", "include",
    "break", "close", "add", "delete", "flush", "show", "for", "while", "return",
    "case", "switch", "typedef", "union", "struct", "enum", "const", "static",
}

# ────────────────────────── OCR 名字修正 ──────────────────────────
#
# 只收能双向确认的：错型是经典 OCR 混淆（l/I、d/l、大小写），且目标名在
# markdown 语料里确实存在。编辑距离猜测一律不做 —— 探测发现 1 字符差的配对里
# 有 PDEVICE_OBJECT/DEVICE_OBJECT、KSJACK_DESCRIPTION2/KSJACK_DESCRIPTION、
# InterlockedAnd8/InterlockedAnd 这类「本来就不同的真实符号」，判据分不开。

OCR_NAME_FIXES = {
    "KelnsertQueueDpc": "KeInsertQueueDpc",                    # l/I 混淆
    "DdMapMemory": "DlMapMemory",                              # d/l 混淆
    "IoUnRegisterBootDriverCallback": "IoUnregisterBootDriverCallback",  # 大小写
}

# 名字可疑但无法确证，只打标记不改名
SUSPECT_NAME_RE = re.compile(
    r"(Lengt|Handl|Inq|Regist|Informat|Structur|Paramete)$"
)

# OCR 侧的类型修正：实测 v4 把一批 HRESULT 类符号标成了 method/structure/macro
# （AUDCLNT_E_BUFFER_TOO_LARGE、STG_E_ACCESSDENIED、TBS_E_BAD_PARAMETER …）。
# 注意反向陷阱：E_POLICY / E_CHECK_POLICY / E_MODE_ENTERED_IN_PARAMS 命中同一
# 正则，但它们其实是 OCR 截断名（描述讲的是 PROCESS_MITIGATION_* 策略结构）。
# 这种不猜原名，类型归到 error_code，另外打 name_suspect 让消费方自己判断。
ERROR_CODE_RE = re.compile(r"^([A-Z0-9_]+_)?E_[A-Z0-9_]+$")
ERROR_WORD_RE = re.compile(r"error|错误|HRESULT|0x8", re.I)

# COM 方法如果丢了接口限定就成了裸名，跨接口会撞名
# （Delete / Copy / GetCount 这些），只打标记，不猜它属于哪个接口
BARE_METHOD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# v4 的 method 分类过宽：实测混进了常量（DISPATCH_LEVEL）、属性
# （RangeMax「获取允许的数据范围的最大值」）、结构体（PDEVICE_CONTEXT
# 的描述里是 C 代码）。归属接口解析不了的这批，按描述证据重定类型。
# 顺序敏感：先看描述形态，再看名字全大写 —— 否则 PDEVICE_CONTEXT
# 会被 isupper() 误判成常量。
PROPERTY_DESC_RE = re.compile(r"^(获取|设置)|该属性|属性返回|属性的")
STRUCT_DESC_RE = re.compile(r"结构|->\s*\w+\s*=|^\s*typedef\b", re.M)


def retype_unresolved_ocr(name: str, desc: str, et: str) -> tuple:
    """返回 (新类型, 依据)；不改则依据为空串。"""
    if et != "method":
        return et, ""
    if STRUCT_DESC_RE.search(desc or ""):
        return "structure", "desc_structure"
    if PROPERTY_DESC_RE.search(desc or ""):
        return "property", "desc_property"
    if name.isupper():
        return "constant", "name_upper_snake"
    return et, ""

# ────────────────────────── 中文描述过滤 ──────────────────────────

CJK_RE = re.compile(r"[一-鿿]")
LONG_LATIN_RE = re.compile(r"[A-Za-z]{22,}")


def description_zh_ok(desc: str, name: str) -> bool:
    """OCR 侧的中文描述是 LLM 改写，实测约 23% 带粘连串或异常，挂之前先筛。"""
    if not desc or len(desc) < 8:
        return False
    if not CJK_RE.search(desc):
        return False
    if LONG_LATIN_RE.search(desc):
        return False
    if desc.strip() == name:
        return False
    return True


# ────────────────────────── 工具 ──────────────────────────

def bare(name: str) -> str:
    """windows::Foo -> Foo；已带其它命名空间的原样返回。"""
    if not isinstance(name, str):
        return name
    if name.startswith("windows::"):
        return name[len("windows::"):]
    return name


def basename_of(path: str) -> str:
    return os.path.basename(path or "")


def prefix_of(basename: str) -> str:
    m = re.match(r"^([a-z]{2})-", basename)
    if not m:
        return ""
    p = m.group(1)
    return p if p in PREFIX_TO_ENTITY_TYPE else ""


def infer_type_from_description(desc: str) -> str:
    for rx, et in DESC_SHAPE_RULES:
        if rx.search(desc or ""):
            return et
    return ""


# markdown 侧的 COM 方法页，uid 是 NF:<头文件>.<接口>.<方法>，
# 但抽出来的实体名只取了「接口」那一段（如 IDebugAdvanced3 实为
# IDebugAdvanced3.FindSourceFileAndToken）。uid 字段 v020 保留着，可以据此还原。
UID_METHOD_RE = re.compile(r"^NF:([^.]+)\.([^.]+)\.(.+)$")

# 源文件 front matter 的 title 是权威类型声明：
#   "ACX_JACK_CONFIG_INIT macro"、"BTHHFP_..._INIT function (bthhfpddi.h)"
# 而 description 是散文，措辞松散得多 —— 同一个文件里 title 写 function、
# description 写 method 的情况实测存在（bthhfpddi 那批）。所以以 title 为准。
# 顺序敏感：callback 要在 function 前，"... callback function" 两个词都命中。
TITLE_KIND_RULES = [
    (re.compile(r"\benumeration\b|\benum\b", re.I), "enum"),
    (re.compile(r"\bunion\b", re.I), "union"),
    (re.compile(r"\bstructure\b", re.I), "structure"),
    (re.compile(r"\bcallback\b", re.I), "callback"),
    (re.compile(r"\bmacro\b", re.I), "macro"),
    (re.compile(r"\bmethod\b", re.I), "method"),
    (re.compile(r"\bioctl\b", re.I), "ioctl"),
    (re.compile(r"\bmessage\b", re.I), "message"),
    (re.compile(r"\binterface\b", re.I), "interface"),
    (re.compile(r"\bclass\b", re.I), "class"),
    (re.compile(r"\bfunction\b", re.I), "function"),
]

_FM_CACHE = {}


def read_front_matter(path: str) -> dict:
    """只读文件头部，取 front matter 里几个关键字段。"""
    if path in _FM_CACHE:
        return _FM_CACHE[path]
    meta = {}
    if path and os.path.isfile(path):
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                head = f.read(2048)
            for key in ("title", "description", "req.header", "ms.date"):
                m = re.search(r"^%s:\s*(.+)$" % re.escape(key), head, re.M)
                if m:
                    meta[key] = m.group(1).strip().strip('"').strip("'")
        except OSError:
            pass
    _FM_CACHE[path] = meta
    return meta


def read_source_title(path: str) -> str:
    return read_front_matter(path).get("title", "")


# 还原被方法页顶掉的接口/类实体时，按这些前缀去找它自己的页面
SIBLING_PREFIXES = ("nn", "nl")


def find_container_page(method_src: str, header: str, iface: str) -> str:
    """
    nf-<header>-<iface>-<method>.md 的同级目录里找 <nn|nl>-<header>-<iface>.md。
    实测 462 个待还原方法里 443 个能这样找回容器页，其余是 nl- 类页。
    """
    if not method_src:
        return ""
    d = os.path.dirname(method_src)
    for pfx in SIBLING_PREFIXES:
        cand = os.path.join(d, "%s-%s-%s.md" % (pfx, header.lower(), iface.lower()))
        if os.path.isfile(cand):
            return cand
    return ""


def title_declared_type(title: str, name: str) -> str:
    """
    标题里明确声明了类型、且标题确实在讲这个实体时才返回类型，否则返回空串。
    要求标题含实体名，避免 "Using interfaces" 这类泛指把类型带歪。
    """
    if not title:
        return ""
    if name.lower() not in title.lower():
        return ""
    hits = [et for rx, et in TITLE_KIND_RULES if rx.search(title)]
    if len(hits) != 1:
        return ""
    return hits[0]


def qualified_method_name(ent: dict, name: str) -> str:
    """uid 显示这是接口方法但名字被截成接口名时，返回限定名；否则返回空串。"""
    m = UID_METHOD_RE.match(ent.get("uid") or "")
    if not m or m.group(2) != name:
        return ""
    return "%s.%s" % (m.group(2), m.group(3))


# ────────────────────────── markdown 侧 ──────────────────────────

def classify_markdown_entity(name: str, ent: dict, report: dict) -> tuple:
    """
    返回 (entity_type, keep, drop_reason)。
    类型优先级：文件名前缀 > 描述形态 > 现有字段证据 > unknown
    """
    src = ent.get("source_file") or ""
    b = basename_of(src)
    desc = ent.get("description") or ""

    # ---- 垃圾判据 ----
    if DROP_DESC_RE.match(desc):
        return "", False, "portal_desc"
    if b in DROP_BASENAMES:
        return "", False, "list_page:%s" % b
    if DROP_BASENAME_RE.search(b):
        return "", False, "concept_page:%s" % b
    if b and re.search(r"\.h header$", desc, re.I) and not b.startswith(("nf-", "ns-", "nc-", "ne-", "ni-")):
        return "", False, "header_page"
    if name.lower() in DROP_NAMES:
        return "", False, "c_token"

    pfx = prefix_of(b)
    has_payload = bool(ent.get("parameters") or ent.get("return_type") or ent.get("header"))

    # 无类型前缀 + 无负载 + 名字不像标识符 -> 概念页标题
    if not pfx and not has_payload:
        if not re.search(r"[_A-Z]", name[1:] if len(name) > 1 else ""):
            return "", False, "concept_title"

    # ---- 类型判定 ----
    if pfx:
        et = PREFIX_TO_ENTITY_TYPE[pfx]
        if pfx == "nf" and ("::" in name or "." in name):
            et = "method"                      # IDebugHostField::GetName 这类 COM 方法
        elif pfx == "ne" and "FLAG" in name.upper():
            et = "flags"                       # _WDF_TASK_SEND_OPTIONS_FLAGS 这类
    else:
        et = infer_type_from_description(desc)
        if not et:
            if ent.get("parameters") or ent.get("return_type"):
                et = "function"
            elif name.isupper():
                et = "constant"
            else:
                et = "unknown"
        report["no_prefix_kept"][et] += 1

    return normalize_entity_type(et), True, ""


def load_source(path: str) -> tuple:
    """
    接受两种形态：
      * 一个目录（含 global_entity_index.json + global_edges.json）
      * 一个抽取产物 JSON（含 entities + edges，如 markdown_batch 的输出）
    """
    if os.path.isdir(path):
        idx = json.load(open(os.path.join(path, "global_entity_index.json"), encoding="utf-8"))
        edges_doc = json.load(open(os.path.join(path, "global_edges.json"), encoding="utf-8"))
    else:
        doc = json.load(open(path, encoding="utf-8"))
        idx = {"entities": doc["entities"]}
        edges_doc = {"edges": doc["edges"]}
    return idx, edges_doc


def build_markdown_side(md_dir: str, report: dict, repo: str = "win32-ddi") -> tuple:
    idx, edges_doc = load_source(md_dir)

    entities = {}
    source_remap = {}
    # 还原接口实体前要先知道哪些名字会有主。即将被改名的那批容器名不算「有主」
    # —— 它们的名字正是要被让出来的，正好留给从容器页找回的接口实体。
    _entries = [(ent.get("name") or bare(k), ent) for k, ent in idx["entities"].items()]
    _requalifying = {n for n, ent in _entries if qualified_method_name(ent, n)}
    seen_md_names = {n for n, _ in _entries} - _requalifying
    recovered = {}

    for key, ent in idx["entities"].items():
        name = ent.get("name") or bare(key)

        # 先把被截成接口名的 COM 方法还原成限定名，再判类型
        qualified = qualified_method_name(ent, name)
        forced_method = bool(qualified)
        if forced_method:
            container = name                       # 被顶掉的接口/类实体
            name = qualified
            report["md_method_requalified"] += 1
            source_remap[container] = qualified    # 边的源跟着方法走
            # 把接口/类实体从它自己的页面找回来（否则边没处落）
            if container not in recovered and container not in seen_md_names:
                sib = find_container_page(ent.get("source_file") or "",
                                          UID_METHOD_RE.match(ent.get("uid") or "").group(1),
                                          container)
                if sib:
                    fm = read_front_matter(sib)
                    sib_pfx = prefix_of(basename_of(sib))
                    sib_type = PREFIX_TO_ENTITY_TYPE.get(sib_pfx, "unknown")
                    # 容器页有些是门户式文案（"Learn more about: CWiaLogProc class"）。
                    # 节点要留着给边落，但这种描述没有信息量，清空而不是原样带进来。
                    sib_desc = fm.get("description", "")
                    if DROP_DESC_RE.match(sib_desc):
                        sib_desc = ""
                        report["md_container_desc_cleared"] += 1
                    recovered[container] = {
                        "id": "windows::%s" % container, "name": container,
                        "type": sib_type, "entity_type": sib_type,
                        "description": sib_desc,
                        "header": fm.get("req.header", ""),
                        "ms_date": fm.get("ms.date", ""),
                        "source_file": sib, "source_format": "markdown",
                        "provenance": "markdown", "confidence": 1.0,
                        "name_verified": True, "recovered_from_container_page": True,
                    }
                    report["md_container_recovered"] += 1
                else:
                    report["md_container_unrecoverable"] += 1

        et, keep, reason = classify_markdown_entity(name, ent, report)
        if not keep:
            report["md_dropped"][reason] += 1
            report["md_dropped_samples"].setdefault(reason, []).append(name)
            continue
        if forced_method:
            et = "method"
        # 源标题是权威：它明确声明了类型且与推导结果不符时，改信标题
        declared = title_declared_type(read_source_title(ent.get("source_file") or ""), name)
        if declared:
            if declared != et:
                report["md_type_from_title_fix"][(et, declared)] += 1
                et = declared
            else:
                report["md_type_title_confirmed"] += 1

        new = dict(ent)
        new["name"] = name
        new["id"] = "windows::%s" % name
        new["type"] = et
        new["entity_type"] = et
        new["provenance"] = "markdown"
        new["source_repo"] = repo
        new["confidence"] = 1.0
        new["name_verified"] = True
        if name in entities:
            report["md_requalify_collision"] += 1
        entities[name] = {"key": key, "ent": new}

    # 把从容器页找回的接口/类实体并进来（它们不是被重命名的那批方法）
    for cname, cent in recovered.items():
        if cname not in entities:
            entities[cname] = {"key": "windows::%s" % cname, "ent": cent}

    # 累加而不是覆盖 —— 这个函数会被 sdk-api 和 win32-ddi 两个源各调一次
    report["md_type_fixed"].update(v["ent"]["type"] for v in entities.values())

    kept_edges = []
    for e in edges_doc["edges"]:
        et = normalize_edge_type(e.get("type"))
        if et in ("semantically_related", "related"):
            report["edges_dropped_weak"] += 1
            continue
        s, t = bare(e.get("source")), bare(e.get("target"))
        # 源是被还原的方法页的边跟着方法走；目标留着指向接口实体（已从容器页找回）
        s = source_remap.get(s, s)
        if not s or not t or s == t:
            report["edges_dropped_selfloop"] += 1
            continue
        kept_edges.append({"source": s, "target": t, "type": et,
                           "_count": e.get("_count", 1), "weight": e.get("weight", 1),
                           "provenance": "markdown"})
    return entities, kept_edges


# ────────────────────────── COM 方法归属接口解析 ──────────────────────────
#
# v4 抽 COM 方法时丢了接口限定，536 个方法成了裸名（Delete / Copy / GetCount）。
# 三条证据按优先级取，只有唯一命中的才改名，有歧义就保持裸名并打标记：
#   ① sdk-api / DDI 的 nf-<header>-<interface>-<method>.md 文件名
#   ② v4 中文描述里点名的接口
#   ③ OCR 原文里的 "IInterface：：Method" / "IInterface 的 Method 方法"
#      （OCR 原文是全角冒号，这是它区别于正常 markdown 的特征）

NF_NAME_RE = re.compile(r"^nf-([a-z0-9_]+)-(.+)$")
OCR_QUAL_RE = re.compile(r"(I[A-Z][A-Za-z0-9_]{2,})\s*[：:]{2}\s*([A-Za-z_][A-Za-z0-9_]*)")
OCR_ZH_RE = re.compile(r"(I[A-Z][A-Za-z0-9_]{2,})\s*(?:接口)?\s*的\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:方法|函数)")
IFACE_IN_DESC_RE = re.compile(r"\b(I[A-Z][A-Za-z0-9]{2,})\b")
OCR_FILE_PREFIX = "[OCR]_windows-"


def build_method_index(corpus_dirs) -> tuple:
    """
    返回 (by_hdr_method, by_method)：
      by_hdr_method[(header, method)] -> {接口小写: 接口原名}
      by_method[method]              -> {接口小写: 接口原名}
    header 取自 sdk-api / DDI 的目录名（即 nf-<header>-... 的首段，等同于 <header>.h）。
    """
    by_hm = collections.defaultdict(dict)
    by_m = collections.defaultdict(dict)
    for root in corpus_dirs:
        if not root or not os.path.isdir(root):
            continue
        for _dp, _dn, fs in os.walk(root):
            for fn in fs:
                if not fn.startswith("nf-") or not fn.endswith(".md"):
                    continue
                m = NF_NAME_RE.match(fn[:-3])
                if not m:
                    continue
                hdr, rest = m.group(1).lower(), m.group(2)
                if "-" not in rest:
                    continue
                iface, method = rest.split("-", 1)
                method = method.split("(")[0]
                if not (iface and method):
                    continue
                by_hm[(hdr, method.lower())][iface.lower()] = iface
                by_m[method.lower()][iface.lower()] = iface
    return by_hm, by_m


# 分文档 / Pass-1 文件名里的 topic：<前缀>_<topic>_<日期>_<时分>.json
DOC_TOPIC_RE = re.compile(r"^(.+?)_([a-z0-9_]+)_\d{8}_\d{4}\.json$")


def doc_key(fn: str) -> tuple:
    """返回 (domain, topic)。'win32-api-_stg_2026...json' -> ('win32-api','stg')"""
    m = DOC_TOPIC_RE.match(fn)
    if not m:
        return "", ""
    return m.group(1).rstrip("-_"), m.group(2)


def load_pass1_entities(ocr_dir: str) -> dict:
    """
    Pass-1 产物（`_p_*.json` = .p.txt 那一路，`_t_*.json` = .txt 那一路）。

    实测：70 个「择优」分档文件是 Pass-1 的**真子集** —— 择优那步只选了一路，
    另一路的实体整个没进后续流程。Pass-1 独有 4,679 个实体名，其中 3,894 个
    连 sdk-api / DDI 都没有（多是 Win32 常量、标志位、枚举值，sdk-api 不给这些
    建独立页面）。所以清理 Pass-1 之前必须先把这批捞出来。
    返回 {topic: {name: entity}}。
    """
    by_topic = {}
    if not os.path.isdir(ocr_dir):
        return by_topic
    for fn in os.listdir(ocr_dir):
        if not re.match(r"^_[pt]_.*\.json$", fn):
            continue
        try:
            doc = json.load(open(os.path.join(ocr_dir, fn), encoding="utf-8"))
        except (OSError, ValueError):
            continue
        _domain, topic = doc_key(fn)
        if not topic:
            continue
        slot = by_topic.setdefault(topic, {})
        for e in doc.get("entities", []):
            n = e.get("name")
            if n and n not in slot:
                slot[n] = e
    return by_topic


def load_ocr_perdoc(ocr_dir: str) -> dict:
    """
    v4 的分文档 JSON 比汇总索引丰富得多：header / confidence / syntax /
    parameters / return_value / cross_references / _source_line 都在这里，
    汇总索引只留了 5 个字段。裸方法名的 header 是精确消歧信号，必须捞回来。
    """
    per = {}
    for fn in os.listdir(ocr_dir):
        if fn.startswith(("_", "global_")) or not fn.endswith(".json"):
            continue
        try:
            doc = json.load(open(os.path.join(ocr_dir, fn), encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for e in doc.get("entities", []):
            per[(fn, e.get("name") or "")] = e
    return per


def load_ocr_context(ocr_root: str) -> dict:
    """v4 单文档 JSON 名 -> OCR 全文（用于找回丢失的接口限定）"""
    ctx = {}
    if not os.path.isdir(ocr_root):
        return ctx
    for fn in os.listdir(ocr_root):
        if fn.startswith(OCR_FILE_PREFIX) and fn.endswith(".txt") and not fn.endswith(".p.txt"):
            key = fn[len(OCR_FILE_PREFIX):-4]
            try:
                ctx[key] = open(os.path.join(ocr_root, fn), encoding="utf-8", errors="replace").read()
            except OSError:
                pass
    return ctx


# 实体在 OCR 原文里的行号附近取多大窗口找接口名
CTX_WINDOW = 60


def resolve_bare_method(name: str, desc: str, src_file: str, header: str,
                        method_index: tuple, ocr_ctx: dict, source_line: int = 0) -> tuple:
    """
    返回 (限定名, 依据)；解析不出返回 ('', 原因)。

    证据按可靠性排序，只有唯一命中才改名：
      ① v4 自带的 header + 方法名 -> sdk-api 目录名匹配（最可靠，实测覆盖 76%）
      ② ① 命中多个接口时，用描述/OCR 原文里点名的接口收敛
      ③ 全局文件名候选唯一
      ④ 描述 ∩ 候选 / OCR 原文 ∩ 候选
      ⑤ 全文仍歧义时，只看 _source_line 附近的窗口（同一文档里多个同名方法
         分属不同接口时，全文扫描会混在一起，按行号定位才分得开）
    """
    by_hm, by_m = method_index if method_index else ({}, {})
    low = name.lower()

    def pick(cands):
        return next(iter(cands.values())) if len(cands) == 1 else ""

    desc_ifaces = {m.lower() for m in IFACE_IN_DESC_RE.findall(desc or "")}
    key = src_file[:-5] if src_file.endswith(".json") else src_file
    text = ocr_ctx.get(key) or ""
    ctx_ifaces = set()
    if text:
        for rx in (OCR_QUAL_RE, OCR_ZH_RE):
            for m in rx.finditer(text):
                if m.group(2).lower() == low:
                    ctx_ifaces.add(m.group(1).lower())
    named = desc_ifaces | ctx_ifaces

    # ① header + 方法名
    if header:
        h = header.lower()
        if h.endswith(".h"):
            h = h[:-2]
        cands = by_hm.get((h, low)) or {}
        got = pick(cands)
        if got:
            return "%s.%s" % (got, name), "header_unique"
        # ② 同 header 下多接口同名 -> 用点名接口收敛
        if len(cands) > 1 and named:
            narrowed = {k: v for k, v in cands.items() if k in named}
            got = pick(narrowed)
            if got:
                return "%s.%s" % (got, name), "header_and_name"

    # ③ 全局唯一
    cands = by_m.get(low) or {}
    got = pick(cands)
    if got:
        return "%s.%s" % (got, name), "global_unique"

    # ④ 描述 / OCR 原文全文收敛
    if cands and named:
        narrowed = {k: v for k, v in cands.items() if k in named}
        got = pick(narrowed)
        if got:
            return "%s.%s" % (got, name), "context_unique"

    # ⑤ 只看 _source_line 附近的窗口
    if cands and text and source_line:
        lines = text.splitlines()
        lo = max(0, source_line - CTX_WINDOW)
        hi = min(len(lines), source_line + CTX_WINDOW)
        window = "\n".join(lines[lo:hi])
        win_ifaces = set()
        for rx in (OCR_QUAL_RE, OCR_ZH_RE, IFACE_IN_DESC_RE):
            for m in rx.finditer(window):
                win_ifaces.add(m.group(1).lower())
        narrowed = {k: v for k, v in cands.items() if k in win_ifaces}
        got = pick(narrowed)
        if got:
            return "%s.%s" % (got, name), "source_line_window"

    if not cands:
        return "", "no_candidate"
    return "", "ambiguous:%d" % len(cands)


# ────────────────────────── OCR 侧 ──────────────────────────

def pass1_entity_ok(name: str, ent: dict) -> bool:
    """Pass-1 实体也过一遍垃圾判据，口径与 markdown 侧保持一致。"""
    if not name or name.lower() in DROP_NAMES:
        return False
    if DROP_DESC_RE.match(ent.get("description") or ""):
        return False
    return True


def build_ocr_side(ocr_dir: str, report: dict,
                   method_index: tuple = None, ocr_ctx: dict = None,
                   pass1: dict = None) -> tuple:
    idx = json.load(open(os.path.join(ocr_dir, "global_entity_index.json"), encoding="utf-8"))
    edges_doc = json.load(open(os.path.join(ocr_dir, "global_edges.json"), encoding="utf-8"))
    perdoc = load_ocr_perdoc(ocr_dir)

    entities = {}
    # 改名映射：边要跟着新名走
    rename = {}
    for key, ent in idx["entities"].items():
        name = ent.get("name") or bare(key)
        orig = name
        fixed = OCR_NAME_FIXES.get(name)
        if fixed:
            report["ocr_renamed"][name] = fixed
            name = fixed
        et = normalize_entity_type(ent.get("type") or "unknown")

        # 汇总索引只有 5 个字段，把分文档里的富字段捞回来
        rec = perdoc.get((ent.get("file", ""), orig)) or {}
        header = rec.get("header") or (rec.get("requirements") or {}).get("header") or ""

        # HRESULT 类符号被误标成别的类型的，改回 error_code
        is_err_name = bool(ERROR_CODE_RE.match(name))
        if is_err_name and et != "error_code":
            et = "error_code"
            report["ocr_retyped_error_code"] += 1
        # 描述里没有 error/错误/HRESULT 措辞的，名字多半被截断了
        if is_err_name and not ERROR_WORD_RE.search(ent.get("description") or ""):
            report["ocr_err_code_name_suspect"].append(name)

        new = {
            "id": "windows::%s" % name,
            "name": name,
            "type": et,
            "entity_type": et,
            "file": ent.get("file", ""),
            "description": ent.get("description", ""),
            "provenance": "ocr",
            "name_verified": False,
        }
        if header:
            new["header"] = header
        if rec.get("confidence") is not None:
            new["confidence"] = rec["confidence"]
        if rec.get("deprecated"):
            new["deprecated"] = True
        if rec.get("syntax"):
            new["syntax"] = rec["syntax"]
        if rec.get("parameters"):
            new["parameters"] = rec["parameters"]
        if rec.get("return_value"):
            new["return_value"] = rec["return_value"]

        # 丢了接口限定的 COM 方法：用 header / 文件名 / 描述 / OCR 原文找回归属接口
        if et == "method" and BARE_METHOD_RE.match(name) and not ERROR_CODE_RE.match(name):
            report["ocr_bare_method"] += 1
            qualified, why = ("", "no_index")
            if method_index:
                qualified, why = resolve_bare_method(name, new["description"], new["file"],
                                                     header, method_index, ocr_ctx or {},
                                                     rec.get("_source_line") or 0)
            if qualified:
                report["ocr_method_resolved"] += 1
                report["ocr_method_by_reason"][why] += 1
                rename[orig] = qualified
                name = qualified
                new["name"] = name
                new["id"] = "windows::%s" % name
                new["name_qualified"] = True
                new["name_resolved_by"] = why
            else:
                # 归属接口定不了，再判一次它到底是不是方法
                et2, why2 = retype_unresolved_ocr(name, new["description"], et)
                if why2:
                    report["ocr_retyped_from_method"][why2] += 1
                    et = et2
                    new["type"] = et
                    new["entity_type"] = et
                    new["retyped_from"] = "method"
                else:
                    new["name_qualified"] = False
                    report["ocr_method_unresolved"][why.split(":")[0]] += 1
                    report["ocr_bare_method_unresolved"].append((name, why))
        if SUSPECT_NAME_RE.search(name) or name in report["ocr_err_code_name_suspect"]:
            new["name_suspect"] = True
            report["ocr_name_suspect"].append(name)
        entities[name] = new

    cross_edges = []

    # Pass-1 独有的实体并入。择优那步只选了一路，另一路的实体整个没进后续流程，
    # 其中 3,894 个连 sdk-api / DDI 都没有（Win32 常量与标志位居多）。
    p1_added = 0
    for topic, slot in (pass1 or {}).items():
        for name, e in slot.items():
            # Pass-1 也要走同一张名字修正表，否则改过的错名会从这一路重新漏回来
            fixed = OCR_NAME_FIXES.get(name)
            if fixed:
                report["ocr_renamed"][name] = fixed
                name = fixed
            if name in entities or not pass1_entity_ok(name, e):
                continue
            et = normalize_entity_type(e.get("entity_type") or "unknown")
            if ERROR_CODE_RE.match(name) and et != "error_code":
                et = "error_code"
            new = {
                "id": "windows::%s" % name,
                "name": name,
                "type": et,
                "entity_type": et,
                "file": "",                      # 由 _topic 推出，见 emit_pertopic
                "_topic": topic,
                "description": e.get("description", ""),
                "provenance": "ocr_pass1",
                "name_verified": False,
            }
            for f in ("header", "deprecated"):
                if e.get(f):
                    new[f] = e[f]
            if e.get("confidence") is not None:
                new["confidence"] = e["confidence"]
            if e.get("_source_line"):
                new["_source_line"] = e["_source_line"]
            if e.get("cross_references"):
                new["cross_references"] = e["cross_references"]
            if SUSPECT_NAME_RE.search(name):
                new["name_suspect"] = True
            entities[name] = new
            p1_added += 1
    report["ocr_pass1_added"] = p1_added

    # Pass-1 的 cross_references 也是边数据
    for topic, slot in (pass1 or {}).items():
        for name, e in slot.items():
            if name not in entities:
                continue
            for ref in e.get("cross_references") or []:
                t = str(ref).strip()
                if t and t != name:
                    cross_edges.append({"source": name, "target": t, "type": "references",
                                        "_count": 1, "weight": 1, "provenance": "ocr"})

    # cross_references 是分文档里另一份被汇总索引丢掉的边数据
    cross_added = 0
    for key, ent in idx["entities"].items():
        orig = ent.get("name") or bare(key)
        rec = perdoc.get((ent.get("file", ""), orig)) or {}
        src = rename.get(orig, orig)
        for ref in rec.get("cross_references") or []:
            t = str(ref).strip()
            if t and t != src:
                cross_edges.append({"source": src, "target": t, "type": "references",
                                    "_count": 1, "weight": 1, "provenance": "ocr"})
                cross_added += 1
    report["ocr_cross_ref_edges"] = cross_added

    kept_edges = list(cross_edges)
    for e in edges_doc["edges"]:
        et = normalize_edge_type(e.get("type"))
        if et in ("semantically_related", "related"):
            report["edges_dropped_weak"] += 1
            continue
        s, t = bare(e.get("source")), bare(e.get("target"))
        s = rename.get(s, s)
        if not s or not t or s == t:
            report["edges_dropped_selfloop"] += 1
            continue
        kept_edges.append({"source": s, "target": t, "type": et,
                           "_count": e.get("_count", 1), "weight": e.get("weight", 1),
                           "provenance": "ocr"})

    # syntax / return_value 能补出 parameter_type 与 return_type 边
    for key, ent in idx["entities"].items():
        orig = ent.get("name") or bare(key)
        rec = perdoc.get((ent.get("file", ""), orig)) or {}
        src = rename.get(orig, orig)
        if src not in entities:
            continue
        for p in rec.get("parameters") or []:
            pt = p.get("type") if isinstance(p, dict) else None
            if pt:
                kept_edges.append({"source": src, "target": str(pt), "type": "parameter_type",
                                   "_count": 1, "weight": 1, "provenance": "ocr"})
        rv = rec.get("return_value")
        rv = rv.get("type") if isinstance(rv, dict) else (rv if isinstance(rv, str) else None)
        if rv:
            kept_edges.append({"source": src, "target": str(rv), "type": "return_type",
                               "_count": 1, "weight": 1, "provenance": "ocr"})
    return entities, kept_edges


# ────────────────────────── 合并 ──────────────────────────

MD_FIELDS = ("header", "parameters", "return_type", "api_type", "api_location",
             "ms_date", "uid", "tech_root", "api_topic", "ms_assetid", "source_file")


def merge_markdown_sources(primary: dict, secondary: dict, report: dict) -> dict:
    """
    两个官方 markdown 源合并。primary 是 sdk-api（Win32 权威源），
    secondary 是 desktop-src + DDI 的冻结产物。同名时取字段更全的那份，
    字段数相同则以 primary 为准。
    """
    out = dict(primary)
    for name, ent in secondary.items():
        if name not in out:
            out[name] = ent
            continue
        report["md_cross_source_dup"] += 1
        if len(ent.get("ent", {})) > len(out[name].get("ent", {})):
            out[name] = ent
    return out


def merge_entities(md_ents: dict, ocr_ents: dict, report: dict) -> dict:
    out = {}
    for name, md in md_ents.items():
        ent = dict(md["ent"])
        o = ocr_ents.get(name)
        if o:
            report["overlap"] += 1
            ent["provenance"] = "markdown+ocr"
            zh = o.get("description") or ""
            if description_zh_ok(zh, name):
                ent["description_zh"] = zh
                report["zh_attached"] += 1
            else:
                report["zh_rejected"] += 1
            if o.get("type") != ent.get("type"):
                report["type_conflict"][(o.get("type"), ent.get("type"))] += 1
        out[name] = ent

    ocr_only = 0
    for name, o in ocr_ents.items():
        if name in out:
            continue
        ent = dict(o)
        # OCR 侧没有 header/params 字段，但把中文描述留在 description 上
        # （markdown 侧同名实体的 description 才是权威英文原文）
        ocr_only += 1
        out[name] = ent
    report["ocr_only"] = ocr_only
    report["md_only"] = len(md_ents) - report["overlap"]
    return out


def merge_edges(md_edges: list, ocr_edges: list, report: dict) -> list:
    seen = set()
    out = []
    for e in md_edges + ocr_edges:
        k = (e["source"], e["target"], e["type"])
        if k in seen:
            report["edges_deduped"] += 1
            continue
        seen.add(k)
        out.append(e)
    return out


# 目标天然是「另一类东西」的边：补占位节点，命名空间独立，绝不混进 windows::
PLACEHOLDER_NS = {
    "parameter_type": "type",
    "return_type": "type",
    "uses_type": "type",
    "belongs_to_header": "header",
    "belongs_to_domain": "domain",
}


def emit_pertopic(entities: dict, out_dir: str, report: dict) -> None:
    """
    产出 json_output_v5/pertopic/*.json，一个 topic 一个文件。

    为什么需要：graph_viewer/main.go:111 的 readEntitySourceLine 会去
    <dataDir>/<entity.file> 读分档 JSON 取 _source_line，再回 OCR_raw 摘原文；
    scripts/evaluate_graph_metrics.py:2146 的 --json-pattern 也吃这个形状
    （需要 document 块 + 每实体带 cross_references / confidence / _source_line）。
    分档文件放进 v5 之后，这两个消费方都不再依赖 OCR 的原始 Pass-1 产物。
    """
    d = os.path.join(out_dir, "pertopic")
    os.makedirs(d, exist_ok=True)
    by_topic = collections.defaultdict(list)
    for name, v in entities.items():
        if v.get("provenance") not in ("ocr", "ocr_pass1", "markdown+ocr"):
            continue
        topic = v.get("_topic") or ""
        if not topic:
            _domain, topic = doc_key(v.get("file") or "")
        if topic:
            by_topic[topic].append((name, v))

    for topic, items in sorted(by_topic.items()):
        rel = "pertopic/_v5_%s.json" % topic
        ents = []
        for name, v in sorted(items):
            e = {
                "id": v.get("id"), "name": name,
                "entity_type": v.get("type"),
                "description": v.get("description", ""),
            }
            for f in ("header", "confidence", "deprecated", "_source_line", "cross_references"):
                if v.get(f) is not None:
                    e[f] = v[f]
            e["file"] = rel               # 让 viewer 能按 <dataDir>/<file> 找到本文件
            ents.append(e)
        with open(os.path.join(d, "_v5_%s.json" % topic), "w", encoding="utf-8") as f:
            json.dump({
                "_schema": "v5_pertopic",
                "document": {
                    "document_id": "v5::%s" % topic,
                    "topic": topic,
                    "domain": doc_key(next(iter(items))[1].get("file") or "")[0]
                    or next(iter(items))[1].get("_domain", ""),
                    "source_file": next(iter(items))[1].get("file", ""),
                },
                "entities": ents,
            }, f, ensure_ascii=False, indent=2)
        # 同步实体上的 file 字段，viewer 就是靠它定位的
        for name, v in items:
            v["file"] = rel
    report["pertopic_files"] = len(by_topic)
    report["pertopic_entities"] = sum(len(v) for v in by_topic.values())


def resolve_edges(entities: dict, edges: list, report: dict) -> list:
    """
    把边收敛成「两端都能落到节点上」的可用状态。

    分三种情况：
      1. 源不是实体 -> 边无意义，丢弃（多半来自被剔除的门户页实体）
      2. 目标是 PLACEHOLDER_NS 里的类别（类型/头文件/域）-> 建独立命名空间占位节点
      3. 目标本该是个 API 实体却查无此节点 -> 丢弃并记账
         （markdown 的 references 目标大量是 See-also 链接文字，如
          'Direct3D 12 Reference'、'**CHARACTERISTICS**'，不是 API 名，留着无用）
    """
    names = set(entities)

    kept_src, dropped_src, dropped_self = [], 0, 0
    for e in edges:
        if e["source"] not in names:
            dropped_src += 1
            continue
        # 自环统一在这里收口。各来源自己只过滤了一部分，
        # 新加的 parameter_type / cross_references 没过滤，
        # 会漏出 X --parameter_type--> X 这种零信息量的边。
        if e["source"] == e["target"]:
            dropped_self += 1
            continue
        kept_src.append(e)
    report["edges_dropped_no_source"] = dropped_src
    report["edges_dropped_selfloop"] += dropped_self

    # 收集需要占位的目标
    need = collections.defaultdict(set)
    for e in kept_src:
        t = e["target"]
        if t in names:
            continue
        ns = PLACEHOLDER_NS.get(e["type"])
        if ns:
            need[ns].add(t)

    for ns, targets in need.items():
        for t in sorted(targets):
            # 已有 'domain:menurc' 这类单冒号前缀的，统一成双冒号
            clean = t.split(":", 1)[1] if t.startswith(ns + ":") else t
            entities["%s::%s" % (ns, clean)] = {
                "id": "%s::%s" % (ns, clean), "name": clean,
                "type": ns, "entity_type": ns,
                "description": "", "provenance": "placeholder", "name_verified": False,
            }
        report["placeholder_%s" % ns] = len(targets)

    # 重新指向 + 丢弃仍然落空的
    out = []
    dropped_tgt = collections.Counter()
    for e in kept_src:
        t = e["target"]
        if t in names:
            out.append(e)
            continue
        ns = PLACEHOLDER_NS.get(e["type"])
        if ns:
            clean = t.split(":", 1)[1] if t.startswith(ns + ":") else t
            e["target"] = "%s::%s" % (ns, clean)
            out.append(e)
        else:
            dropped_tgt[e["type"]] += 1
    report["edges_dropped_no_target"] = dropped_tgt
    return out


def count_dangling(entities: dict, edges: list) -> int:
    names = set(entities)
    return sum(1 for e in edges if e["source"] not in names or e["target"] not in names)


# ────────────────────────── 主流程 ──────────────────────────

def main():
    ap = argparse.ArgumentParser(description="合并 markdown + OCR 两个来源，产出 v5 并集库")
    ap.add_argument("--ocr-dir", default="json_output_v4")
    ap.add_argument("--md-dir", default="json_output_v4_v020_markdown",
                    help="desktop-src + DDI 的冻结产物")
    ap.add_argument("--sdkapi", default=SDKAPI_DEFAULT,
                    help="sdk-api 抽取产物 JSON（Win32 桌面 API 权威源）")
    ap.add_argument("--ocr-root", default="OCR_raw",
                    help="OCR 原文目录，用于找回丢失的接口限定")
    ap.add_argument("--pass1-dir", default=None,
                    help="OCR Pass-1 产物目录（默认同 --ocr-dir），择优那步丢掉的另一路实体在这里")
    ap.add_argument("--sdkapi-src", default=SDKAPI_SRC_DEFAULT)
    ap.add_argument("--ddi-src", default=DDI_SRC_DEFAULT)
    ap.add_argument("--output-dir", default="json_output_v5")
    args = ap.parse_args()

    os.chdir(_ROOT)

    report = collections.defaultdict(int)
    report["md_dropped"] = collections.Counter()
    report["md_dropped_samples"] = {}
    report["md_type_fixed"] = collections.Counter()
    report["ocr_renamed"] = {}
    report["ocr_name_suspect"] = []
    report["type_conflict"] = collections.Counter()
    report["ocr_retyped_error_code"] = 0
    report["ocr_bare_method"] = 0
    report["ocr_method_resolved"] = 0
    report["ocr_method_by_reason"] = collections.Counter()
    report["ocr_method_unresolved"] = collections.Counter()
    report["ocr_bare_method_unresolved"] = []
    report["ocr_retyped_from_method"] = collections.Counter()
    report["md_cross_source_dup"] = 0
    report["ocr_err_code_name_suspect"] = []
    report["ocr_cross_ref_edges"] = 0
    report["md_method_requalified"] = 0
    report["md_requalify_collision"] = 0
    report["md_type_from_title_fix"] = collections.Counter()
    report["md_type_title_confirmed"] = 0
    report["md_container_recovered"] = 0
    report["md_container_unrecoverable"] = 0
    report["md_container_desc_cleared"] = 0
    report["no_prefix_kept"] = collections.Counter()
    report["edges_dropped_weak"] = 0
    report["edges_dropped_selfloop"] = 0
    report["edges_deduped"] = 0
    report["overlap"] = 0

    sdk_ents, sdk_edges = build_markdown_side(args.sdkapi, report, repo="sdk-api")
    md_ents, md_edges = build_markdown_side(args.md_dir, report, repo="win32-ddi")
    print("sdk-api 侧实体 %d / 边 %d" % (len(sdk_ents), len(sdk_edges)))
    # 两个官方 markdown 源合并，sdk-api 优先
    md_ents = merge_markdown_sources(sdk_ents, md_ents, report)
    md_edges = sdk_edges + md_edges

    method_index = build_method_index([args.sdkapi_src, args.ddi_src])
    ocr_ctx = load_ocr_context(args.ocr_root)
    print("COM 方法索引: %d 个 (header,方法) 组合 / %d 个方法名 / OCR 原文 %d 篇"
          % (len(method_index[0]), len(method_index[1]), len(ocr_ctx)))
    pass1 = load_pass1_entities(args.pass1_dir or args.ocr_dir)
    print("Pass-1 索引: %d 个 topic" % len(pass1))
    ocr_ents, ocr_edges = build_ocr_side(args.ocr_dir, report,
                                         method_index=method_index, ocr_ctx=ocr_ctx,
                                         pass1=pass1)
    print("markdown 侧实体 %d (原 %d) / 边 %d" % (len(md_ents), len(md_ents) + sum(report["md_dropped"].values()), len(md_edges)))
    print("OCR 侧实体 %d / 边 %d" % (len(ocr_ents), len(ocr_edges)))

    entities = merge_entities(md_ents, ocr_ents, report)
    edges = merge_edges(md_edges, ocr_edges, report)
    edges = resolve_edges(entities, edges, report)
    emit_pertopic(entities, os.path.join(_ROOT, args.output_dir), report)

    report["final_entities"] = len(entities)
    report["final_edges"] = len(edges)
    report["dangling_after"] = count_dangling(entities, edges)
    report["api_entities"] = sum(1 for v in entities.values() if v.get("provenance") != "placeholder")

    out = os.path.join(_ROOT, args.output_dir)
    os.makedirs(out, exist_ok=True)

    with open(os.path.join(out, "global_entity_index.json"), "w", encoding="utf-8") as f:
        json.dump({
            "_schema": "global_entity_index_v5.0_union",
            "_source": "MicrosoftDocs/win32 desktop-src + windows-driver-docs-ddi (markdown) + OCR PDF (Win32)",
            "entities": dict(sorted(entities.items())),
            "total_unique_entities": len(entities),
        }, f, ensure_ascii=False, indent=2)

    with open(os.path.join(out, "global_edges.json"), "w", encoding="utf-8") as f:
        json.dump({
            "_schema": "global_edges_v5.0_union",
            "edges": sorted(edges, key=lambda e: (e["source"], e["target"], e["type"])),
            "total_edges": len(edges),
        }, f, ensure_ascii=False, indent=2)

    rep = {
        "md_dropped_by_reason": dict(report["md_dropped"]),
        "md_dropped_samples": {k: v[:20] for k, v in report["md_dropped_samples"].items()},
        "md_entity_type_after_fix": dict(report["md_type_fixed"]),
        "md_no_prefix_kept_by_inferred_type": dict(report["no_prefix_kept"]),
        "md_method_requalified": report["md_method_requalified"],
        "md_method_requalified_note": "COM 方法页的实体名被截成接口名，按 uid 字段还原为 Interface.Method",
        "md_container_recovered": report["md_container_recovered"],
        "md_container_unrecoverable": report["md_container_unrecoverable"],
        "md_container_desc_cleared": report["md_container_desc_cleared"],
        "md_container_note": "被方法页顶掉的接口/类实体，从同级 <nn|nl>-<header>-<name>.md 恢复",
        "md_requalify_collision": report["md_requalify_collision"],
        "md_type_title_confirmed": report["md_type_title_confirmed"],
        "md_type_corrected_by_source_title": {
            "%s -> %s" % k: v for k, v in report["md_type_from_title_fix"].most_common()},
        "md_type_title_note": "源文件 front matter 的 title 是权威类型声明，与文件名前缀推导冲突时以 title 为准",
        "ocr_renamed": report["ocr_renamed"],
        "ocr_name_suspect": report["ocr_name_suspect"],
        "ocr_retyped_error_code": report["ocr_retyped_error_code"],
        "ocr_err_code_name_suspect": report["ocr_err_code_name_suspect"],
        "ocr_err_code_name_suspect_note": "命中 HRESULT 命名但描述无 error/错误/HRESULT/0x 措辞。低精度标记：多数是真错误码只是描述没提「错误」，少数才是 OCR 截断名（如 E_POLICY 实为 PROCESS_MITIGATION_*_POLICY）。一律未猜原名",
        "ocr_bare_method_names": report["ocr_bare_method"],
        "ocr_method_resolved": report["ocr_method_resolved"],
        "ocr_method_resolved_by_reason": dict(report["ocr_method_by_reason"]),
        "ocr_method_unresolved_by_reason": dict(report["ocr_method_unresolved"]),
        "ocr_retyped_from_method": dict(report["ocr_retyped_from_method"]),
        "ocr_retyped_from_method_note": "v4 的 method 分类过宽，归属接口解析不了的按描述证据重定类型",
        "ocr_bare_method_unresolved": report["ocr_bare_method_unresolved"],
        "ocr_cross_ref_edges": report["ocr_cross_ref_edges"],
        "ocr_pass1_added": report["ocr_pass1_added"],
        "ocr_pass1_note": "Pass-1（_p_/_t_）独有实体。70 个择优分档是 Pass-1 的真子集，择优丢掉的另一路只能从这里补",
        "pertopic_files": report["pertopic_files"],
        "pertopic_entities": report["pertopic_entities"],
        "ocr_cross_ref_note": "分文档 JSON 里的 cross_references，汇总索引丢弃了这批边数据",
        "md_cross_source_dup": report["md_cross_source_dup"],
        "ocr_bare_method_note": "v4 抽取 COM 方法时丢了接口限定，这些名字跨接口可能撞名，改名需要接口上下文",
        "overlap_entities": report["overlap"],
        "type_conflict_ocr_vs_md": {"%s -> %s" % k: v for k, v in report["type_conflict"].most_common(30)},
        "description_zh_attached": report["zh_attached"],
        "description_zh_rejected": report["zh_rejected"],
        "edges_dropped_weak": report["edges_dropped_weak"],
        "edges_dropped_selfloop": report["edges_dropped_selfloop"],
        "edges_deduped": report["edges_deduped"],
        "edges_dropped_no_source": report["edges_dropped_no_source"],
        "edges_dropped_no_target_by_type": dict(report["edges_dropped_no_target"]),
        "placeholder_nodes": {ns: report["placeholder_%s" % ns] for ns in ("type", "header", "domain")},
        "final_entities": report["final_entities"],
        "final_api_entities": report["api_entities"],
        "final_edges": report["final_edges"],
        "dangling_edges_after": report["dangling_after"],
    }
    with open(os.path.join(out, "_v5_build_report.json"), "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)

    print("\n=== v5 ===")
    print("  实体 %d (其中 API 实体 %d, 占位 %d)"
          % (rep["final_entities"], rep["final_api_entities"],
             sum(rep["placeholder_nodes"].values())))
    print("  边   %d  (剩余悬空 %d)" % (rep["final_edges"], rep["dangling_edges_after"]))
    print("  markdown 剔除 %d 条" % sum(report["md_dropped"].values()))
    print("  同名交集 %d, 挂上中文描述 %d, 驳回 %d"
          % (rep["overlap_entities"], rep["description_zh_attached"], rep["description_zh_rejected"]))
    print("  markdown 实体类型分布:")
    for t, c in report["md_type_fixed"].most_common():
        print("    %-12s %d" % (t, c))
    print("  输出目录: %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
