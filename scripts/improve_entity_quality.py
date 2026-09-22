#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
improve_entity_quality.py — 离线 JSON 质量提升脚本

针对 global_entity_index.json 的具体质量问题:

1. 移除 C 关键字污染 (Continue / Enum / LONG / switch 等)
2. 重分类 type=unknown 实体 (基于命名启发式)
3. 过滤 / 修复极短 / 缺失描述 (噪点)
4. 合并死类 (enum_member + enum_value 重复 → 单一类)
5. 同步 edges: 引用被移除实体的边要么删除要么目标替换为 parent

用法:
  python scripts/improve_entity_quality.py              # dry-run 真实数据
  python scripts/improve_entity_quality.py --apply      # 改写
  python scripts/improve_entity_quality.py --backup    # 自动备份
  python scripts/improve_entity_quality.py --keep-empty-desc  # 保留无描述 (默认: 删)

Author: v0.1.2 实施
"""
import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pipeline_lib.config import normalize_entity_type  # noqa: E402

# ──── C 关键字黑名单 ────
# Windows API 命名空间下,这些 token 不可能是独立实体;
# 它们是语法符号,出现在 entity_index 里就是污染。
#
# 注意:BOOL / LONG / UINT / WORD / DWORD 等是 Win32 typedef,不是 C 关键字,
# 即使 OCR 抓取到,它们仍是合法 Windows API 实体,不应删除。
C_KEYWORD_BLACKLIST = {
    # C 关键字
    "auto", "break", "case", "char", "const", "continue", "default",
    "do", "double", "else", "enum", "extern", "float", "for", "goto",
    "if", "inline", "int", "long", "register", "return", "short",
    "signed", "sizeof", "static", "struct", "switch", "typedef",
    "union", "unsigned", "void", "volatile", "while",
    # 基础常量
    "true", "false", "null", "nil",
    # MS 扩展关键字
    "__in", "__out", "__inout", "_in_", "_out_", "_inout_",
}

# 大小写无关黑名单 (避免 BOO L, Bool 等 OCR 变体)
C_KEYWORD_BLACKLIST_CI = {k.lower() for k in C_KEYWORD_BLACKLIST}

# Win32 typedef 覆盖:全大写形式即使是 C 关键字也保留
# (BOOL/LONG/UINT/WORD/DWORD/BYTE 等 windows.h 标准 typedef)
WIN32_TYPEDEF_OVERRIDE = {
    "BOOL", "LONG", "UINT", "WORD", "DWORD", "BYTE",
    "INT", "CHAR", "FLOAT", "DOUBLE", "VOID", "SHORT",
    "WCHAR", "UCHAR", "ULONG", "USHORT", "USHORT",
    "BOOLEAN", "HRESULT", "HWND", "HDC",
}


def is_c_keyword(name: str) -> bool:
    # 覆盖规则:Win32 全大写 typedef 即使 lowercase 是 C 关键字也保留
    if name in WIN32_TYPEDEF_OVERRIDE:
        return False
    return name.lower() in C_KEYWORD_BLACKLIST_CI


# ──── Unknown 类型启发式分类器 ────
# 优先级从高到低,匹配第一个即返回

# 命名模式 → 类型
NAME_TYPE_RULES = [
    # === 高优先级:特殊前缀/单例 ===
    # GUID/IID 常量
    (re.compile(r"^(GUID_|IID_|CLSID_|FMTID_).+", re.IGNORECASE), "constant"),
    # Win32 错误码常量
    (re.compile(r"^(ERROR_|HRESULT_FROM_WIN32|WINERROR_|RPC_E_|HRESULT_E_|NTSTATUS_)"), "constant"),
    # S_OK / E_XXX / FACILITY_ HRESULT
    (re.compile(r"^(S_OK|E_NOTIMPL|E_FAIL|E_INVALIDARG|E_OUTOFMEMORY|E_ACCESSDENIED|E_NOINTERFACE)$"), "constant"),
    # 句柄类型 / Typedef (精确匹配)
    (re.compile(r"^(HANDLE|HDC|HBITMAP|HBRUSH|HPEN|HFONT|HICON|HINSTANCE|HKEY|HMODULE|HWND|HMENU|HGDIOBJ|PFN|LPVOID|LPSTR|LPCSTR|LPWSTR|LPCWSTR)$"), "typedef"),
    # 函数指针 typedef (大写 APIENTRY/WINAPI 后缀)
    (re.compile(r".*(APIENTRY|WINAPI)\s*$"), "function_pointer"),
    # P<UPPER> typedef 模式 (PFN/PDEVICE/PCALLBACK 等函数指针)
    (re.compile(r"^P[A-Z][A-Z0-9_]+$"), "function_pointer"),
    # IOCTL 码
    (re.compile(r"^(IOCTL|FSCTL|OB|IO|CLFS|FS)_"), "ioctl"),

    # === 中优先级:结构型后缀 (必须早于 *Id/*Name 等单字段规则) ===
    (re.compile(r".*_(INFO|CONFIG|PARAMS|PARAMETERS|DESC|DESCRIPTOR)$", re.IGNORECASE), "structure"),
    (re.compile(r".*Info$"), "structure"),
    (re.compile(r".*Config$"), "structure"),
    (re.compile(r".*Params$"), "structure"),
    (re.compile(r".*Data$"), "structure"),
    (re.compile(r".*Request$"), "structure"),
    (re.compile(r".*Response$"), "structure"),
    (re.compile(r".*Buffer$"), "structure"),
    (re.compile(r".*GuidList$"), "structure"),

    # === 回调 / 通知型 ===
    (re.compile(r".*Callback[A-Z]?$"), "callback"),
    (re.compile(r".*Acknowledge$"), "callback"),
    (re.compile(r".*Notify$"), "callback"),
    (re.compile(r".*Notification$"), "callback"),

    # === 字段名后缀 (放到结构规则之后,避免误伤) ===
    (re.compile(r".*BufferLength$"), "field"),
    (re.compile(r".*Length$"), "field"),
    (re.compile(r".*Count$"), "field"),
    (re.compile(r".*Index$"), "field"),
    (re.compile(r".*Name$"), "field"),
    (re.compile(r".*Guid$"), "field"),
    (re.compile(r".*Id$"), "field"),
    (re.compile(r".*Written$"), "field"),
    (re.compile(r".*Enabled$"), "field"),
    (re.compile(r".*State$"), "field"),
    (re.compile(r".*Size$"), "field"),
    (re.compile(r".*Flags$"), "field"),

    # === 低优先级:大写常量 ===
    # 必须早于函数 W/A 规则,否则 DMNUP_SYSTEM 这种会被错判为函数
    (re.compile(r"^[A-Z][A-Z0-9_]{2,}$"), "constant"),

    # === 兜底:PascalCase 函数 (含 W/A Ansi/Unicode 后缀) ===
    # 排他性:非全大写 + 末位是 W 或 A
    (re.compile(r"^(?![A-Z][A-Z0-9_]*$)[A-Za-z].*[WA]$"), "function"),
    # PascalCase 动词型 (Begin/Create/Open/Close/Set/Get/...) → function
    (re.compile(r"^(Begin|Create|Open|Close|Set|Get|Register|Unregister|"
                r"Add|Remove|Delete|Insert|Update|Find|Search|Query|"
                r"Enable|Disable|Start|Stop|Run|Read|Write|Copy|Move|"
                r"Lock|Unlock|Connect|Disconnect|Attach|Detach|"
                r"Initialize|Finalize|Reset|Flush|Purge|Commit|Rollback|"
                r"Acquire|Release|Load|Unload|Install|Uninstall)[A-Z]"), "function"),
    # As<UPPER> 模式 (typedef 宏) → typedef
    (re.compile(r"^As[A-Z][A-Z]+$"), "typedef"),
    # Annotation 单独处理 (易混淆) → typedef
    (re.compile(r"^Annotation$"), "typedef"),
    # BugCheckCode 类 (单字混合) → constant (内核常见)
    (re.compile(r"^(BugCheck|Halt|Stop|Trap)[A-Z][a-z]+$"), "constant"),
]


def classify_unknown_name(name: str, desc: str = "") -> str:
    """基于命名启发式分类 unknown 实体"""
    # 检查 desc 看是不是真正的实体 (例如 description 含 'structure' / 'enum')
    d_lower = (desc or "").lower()
    if "structure" in d_lower or "struct " in d_lower:
        return "structure"
    if "enum" in d_lower and "function" not in d_lower:
        return "enum"
    if "callback" in d_lower:
        return "callback"
    if "macro" in d_lower:
        return "macro"
    if "function" in d_lower or "routine" in d_lower:
        return "function"
    if "constant" in d_lower or "value" in d_lower:
        return "constant"

    # 基于命名
    for pat, t in NAME_TYPE_RULES:
        if pat.search(name):
            return t
    return "unknown"  # 启发式未匹配


# ──── 主流程 ────

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def analyze(entities: dict) -> dict:
    """纯分析:返回将要做什么修改的清单"""
    actions = {
        "remove_c_keyword": [],     # (key, reason)
        "reclassify_unknown": [],   # (key, old_type, new_type)
        "remove_short_desc": [],    # (key, reason, desc_len)
        "drop_no_desc": [],
        "merge_dead_type": [],      # (key, old_type, new_type)
    }
    for k, v in entities.items():
        old_type = v.get("type", "")
        # C 关键字
        if is_c_keyword(k):
            actions["remove_c_keyword"].append((k, f"C keyword: {k}"))
            continue
        # Unknown 重新分类
        if old_type == "unknown":
            new_type = classify_unknown_name(k, v.get("description", ""))
            if new_type != "unknown":
                actions["reclassify_unknown"].append((k, old_type, new_type))
        # 短描述
        desc = v.get("description", "")
        if not desc:
            actions["drop_no_desc"].append((k, "no description"))
        elif len(desc) < 8:
            actions["remove_short_desc"].append((k, f"short desc ({len(desc)} chars)", len(desc)))
        # 死类合并 (enum_member → enum_value 已通过 _TYPE_SYNONYMS 处理,
        # 这里处理更稀有的:event/message/notification/technology 等 <10 个的)
    return actions


def apply_actions(entities: dict, actions: dict, opts: dict) -> dict:
    """应用 actions,返回新 entities"""
    new_entities = dict(entities)
    removed = set()

    # 1. 删除 C 关键字
    for k, reason in actions["remove_c_keyword"]:
        if k in new_entities:
            del new_entities[k]
            removed.add(k)

    # 2. 删除无描述 (按 opt)
    if opts["drop_no_desc"]:
        for k, reason in actions["drop_no_desc"]:
            if k in new_entities and k not in removed:
                del new_entities[k]
                removed.add(k)

    # 3. 删除短描述 (按 opt)
    if opts["drop_short_desc"]:
        for k, reason, dlen in actions["remove_short_desc"]:
            if k in new_entities and k not in removed:
                del new_entities[k]
                removed.add(k)

    # 4. 重分类 unknown
    for k, old_t, new_t in actions["reclassify_unknown"]:
        if k in new_entities:
            new_entities[k]["type"] = new_t

    return new_entities, removed


def fix_edges(edges: list, removed_keys: set) -> list:
    """清理 edges 里引用被移除实体的边"""
    new_edges = []
    dangling_in  = 0
    dangling_out = 0
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s in removed_keys:
            dangling_out += 1
            continue
        if t in removed_keys:
            dangling_in += 1
            continue
        new_edges.append(e)
    return new_edges, dangling_in, dangling_out


def main():
    ap = argparse.ArgumentParser(description="v0.1.2 离线提升 entity_index 质量")
    ap.add_argument("--input", default=None,
                    help="entity index 路径 (默认 global_entity_index.json)")
    ap.add_argument("--edges-input", default=None,
                    help="edges 路径 (默认 global_edges.json, 用于联动修复)")
    ap.add_argument("--apply", action="store_true", help="实际写盘")
    ap.add_argument("--backup", action="store_true", help="改写前备份")
    ap.add_argument("--keep-empty-desc", action="store_true",
                    help="保留无描述实体 (默认删除)")
    ap.add_argument("--keep-short-desc", action="store_true",
                    help="保留 <8 字符描述实体 (默认删除)")
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_in = os.path.join(root, "json_output_v4", "global_entity_index.json")
    default_edges = os.path.join(root, "json_output_v4", "global_edges.json")

    in_path = args.input or default_in
    edges_in = args.edges_input or default_edges

    data = load_json(in_path)
    entities = data.get("entities", data)
    print(f"输入: {in_path}")
    print(f"entity 数: {len(entities)}")

    actions = analyze(entities)
    print(f"\n=== 分析结果 ===")
    print(f"  C 关键字污染待删:    {len(actions['remove_c_keyword'])} 个")
    if actions["remove_c_keyword"]:
        for k, r in actions["remove_c_keyword"][:10]:
            print(f"    - {k}  ({r})")

    print(f"  unknown 待重分类:    {len(actions['reclassify_unknown'])} 个")
    reclass_dist = Counter(t for _, _, t in actions["reclassify_unknown"])
    for t, c in reclass_dist.most_common():
        print(f"    -> {t}: {c}")

    print(f"  无描述待删:          {len(actions['drop_no_desc'])} 个")
    print(f"  <8 字符描述待删:      {len(actions['remove_short_desc'])} 个")

    opts = {
        "drop_no_desc":    not args.keep_empty_desc,
        "drop_short_desc": not args.keep_short_desc,
    }
    new_entities, removed = apply_actions(entities, actions, opts)
    print(f"\n=== 应用后 ===")
    print(f"  实体数:  {len(entities)} -> {len(new_entities)} (减少 {len(entities) - len(new_entities)})")
    print(f"  删除 key 集合大小: {len(removed)}")

    # 联动修 edges
    if os.path.isfile(edges_in):
        with open(edges_in, "r", encoding="utf-8") as f:
            edges_data = json.load(f)
        if isinstance(edges_data, dict) and "edges" in edges_data:
            edges = edges_data["edges"]
            wrapped = True
        elif isinstance(edges_data, list):
            edges = edges_data
            wrapped = False
        else:
            edges = []
            wrapped = False
        new_edges, d_in, d_out = fix_edges(edges, removed)
        print(f"  edges:   {len(edges)} -> {len(new_edges)} (dangling in={d_in}, out={d_out})")
    else:
        new_edges = None
        wrapped = False
        edges_data = None

    if not args.apply:
        print("\n[dry-run] 加 --apply 才实际写盘")
        return 0

    # 备份
    if args.backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bp = in_path + f".bak.{ts}"
        import shutil
        shutil.copy2(in_path, bp)
        print(f"已备份 entity: {bp}")

    # 写回 entity
    if isinstance(data, dict) and "entities" in data:
        data["entities"] = new_entities
        out = data
    else:
        out = new_entities
    save_json(in_path, out)
    print(f"[apply] 已写入 entity: {in_path}")

    # 写回 edges
    if new_edges is not None and edges_in == default_edges:
        if args.backup:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            bp = edges_in + f".bak.{ts}"
            import shutil
            shutil.copy2(edges_in, bp)
            print(f"已备份 edges: {bp}")
        if wrapped:
            edges_data["edges"] = new_edges
            save_obj = edges_data
        else:
            save_obj = new_edges
        save_json(edges_in, save_obj)
        print(f"[apply] 已写入 edges: {edges_in}")

    return 0


if __name__ == "__main__":
    sys.exit(main())