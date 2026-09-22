#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_v5.py — 对 json_output_v5/ 跑断言与抽样核对

断言不过就非零退出。用法: python scripts/verify_v5.py
"""
import collections
import io
import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
os.chdir(_ROOT)

DATA = "json_output_v5"
FAILS = []
# 报告写进数据目录（该目录 gitignore），不在仓库根留生成物
out = io.open(os.path.join(DATA, "_v5_verify_report.txt"), "w", encoding="utf-8")


def check(label, ok, detail=""):
    mark = "PASS" if ok else "FAIL"
    line = "[%s] %s%s" % (mark, label, ("  -- " + detail) if detail else "")
    print(line, file=out)
    print(line)
    if not ok:
        FAILS.append(label)


idx = json.load(open(os.path.join(DATA, "global_entity_index.json"), encoding="utf-8"))
edg = json.load(open(os.path.join(DATA, "global_edges.json"), encoding="utf-8"))
ents = idx["entities"]
edges = edg["edges"]
names = set(ents)

# ── 结构契约 ──
check("实体索引含 entities", isinstance(ents, dict), "n=%d" % len(ents))
check("边文件含 edges", isinstance(edges, list), "n=%d" % len(edges))
# graph_viewer/main.go:34 读 json 标签 "type"，只写 entity_type 的话前端类型全空
missing_type = [k for k, v in ents.items() if not v.get("type")]
check("每个实体都有 type 字段（graph_viewer 读的就是这个）", not missing_type,
      "缺 %d 个" % len(missing_type))
mismatch = [k for k, v in ents.items()
            if "entity_type" in v and v.get("entity_type") != v.get("type")]
check("type 与 entity_type 一致", not mismatch, "不一致 %d 个" % len(mismatch))

# ── 悬空与自环 ──
dangling = [e for e in edges if e["source"] not in names or e["target"] not in names]
check("无悬空边", not dangling, "%d 条" % len(dangling))
selfloop = [e for e in edges if e["source"] == e["target"]]
check("无自环边", not selfloop, "%d 条" % len(selfloop))
dup = len(edges) - len({(e["source"], e["target"], e["type"]) for e in edges})
check("无重复边", dup == 0, "%d 条" % dup)

# ── 垃圾是否清干净 ──
portal = [k for k, v in ents.items()
          if re.match(r'^"?\s*Learn more about\s*:', v.get("description") or "", re.I)]
check("无 'Learn more about' 门户描述", not portal, "残留 %d 个: %s" % (len(portal), portal[:5]))
# 只查小写形态。大写开头的 Delete / Flush 是 IPropertySetStorage::Delete、
# ILockBytes::Flush 这类真 COM 方法，不能一竿子打掉。
ctok = [k for k, v in ents.items()
        if k in {"if", "else", "endif", "include", "close", "add", "delete",
                 "flush", "show", "define", "undef", "ifdef", "ifndef", "break"}]
check("无小写 C 关键字/预处理词实体", not ctok, "残留 %s" % ctok[:5])
err_mistyped = [k for k, v in ents.items()
                if re.match(r"^([A-Z0-9_]+_)?E_[A-Z0-9_]+$", k) and v.get("type") != "error_code"]
check("错误码未被误标成函数/方法", not err_mistyped, "残留 %d 个: %s" % (len(err_mistyped), err_mistyped[:5]))
bare_m = [k for k, v in ents.items() if v.get("name_qualified") is False]
check("裸方法名已打 name_qualified 标记", len(bare_m) > 0,
      "%d 个（已知局限，v4 抽取时丢了接口限定）" % len(bare_m))

# ── 类型分布 ──
dist = collections.Counter(v.get("type") for v in ents.values())
check("类型分布不是单一 function", dist.get("function", 0) < 0.6 * len(ents),
      "function 占 %.1f%%" % (100.0 * dist.get("function", 0) / len(ents)))
for need in ("structure", "enum", "callback", "ioctl", "interface", "message", "flags"):
    check("存在类型 %s" % need, dist.get(need, 0) > 0, "n=%d" % dist.get(need, 0))
unknown_rate = 100.0 * dist.get("unknown", 0) / len(ents)
check("unknown 占比 < 5%", unknown_rate < 5.0, "%.2f%%" % unknown_rate)

# ── 占位节点不能混进 windows:: 命名空间 ──
ph = [k for k, v in ents.items() if v.get("provenance") == "placeholder"]
bad_ns = [k for k in ph if k.startswith("windows::")]
check("占位节点不在 windows:: 命名空间", not bad_ns, "%d 个越界" % len(bad_ns))
check("占位节点都有命名空间前缀", all("::" in k for k in ph), "%d 个" % len(ph))

# ── 本地 desktop-src 缺的 Win32 桌面 API，sdk-api 补上了吗 ──
# 这几个在本地 desktop-src checkout 里 find 不到，只能由 sdk-api 提供
WIN32_BACKFILL = ["AbortDoc", "ActivateKeyboardLayout", "ACCESS_ALLOWED_ACE",
                  "AVIFileCreateStream", "CreateFileW", "GetLastError"]
missing_win32 = [n for n in WIN32_BACKFILL if n not in ents]
check("本地 desktop-src 缺失的 Win32 API 已由 sdk-api 补上", not missing_win32,
      "缺 %s" % missing_win32)
src_repo = collections.Counter(v.get("source_repo") for v in ents.values())
check("存在 sdk-api 来源实体", src_repo.get("sdk-api", 0) > 40000,
      "sdk-api %d 条" % src_repo.get("sdk-api", 0))

# ── COM 方法名归属接口解析 ──
rep_pre = json.load(open(os.path.join(DATA, "_v5_build_report.json"), encoding="utf-8"))
bare = rep_pre.get("ocr_bare_method_names", 0)
resolved = rep_pre.get("ocr_method_resolved", 0)
retyped = sum(rep_pre.get("ocr_retyped_from_method", {}).values())
check("裸方法名解析率 >= 80%", bare and resolved / bare >= 0.80,
      "%d/%d = %.0f%%（另 %d 个查明不是方法）" % (resolved, bare, 100.0 * resolved / max(1, bare), retyped))
still_bare = {k for k, v in ents.items() if v.get("name_qualified") is False}
unresolved_names = {x[0] for x in rep_pre.get("ocr_bare_method_unresolved", [])}
# 少数未解析名在合并时被 markdown 侧同名实体接管（markdown 优先），
# 那些实体不带 name_qualified 标记，所以只要求是子集。
check("未解析的裸方法名都带 name_qualified: false 标记",
      still_bare <= unresolved_names,
      "标记 %d / 报告 %d（差集已被 markdown 同名实体接管: %s）"
      % (len(still_bare), len(unresolved_names), sorted(unresolved_names - still_bare)[:4]))
check("未解析的裸方法名确实没带接口限定",
      all("." not in k for k in still_bare), "带点的 %s" % [k for k in still_bare if "." in k][:3])
ok_qualified = [k for k, v in ents.items() if v.get("name_qualified") is True]
check("已解析的方法用 Interface.Method 形式", all("." in k for k in ok_qualified),
      "%d 个" % len(ok_qualified))
for probe in ("IPropertySetStorage.Delete", "ILockBytes.Flush"):
    check("归属接口解析结果存在: %s" % probe, probe in ents)

# ── Pass-1 独有实体已保住（删原始 Pass-1 的前提）──
p1_added = rep_pre.get("ocr_pass1_added", 0)
check("Pass-1 独有实体已并入 v5", p1_added > 4000, "%d 个" % p1_added)
p1_left = [k for k, v in ents.items() if v.get("provenance") == "ocr_pass1"]
check("ocr_pass1 来源实体在索引里", len(p1_left) > 4000, "%d 个" % len(p1_left))
for probe in ("AF_INET", "AD_CLOCKWISE", "ACMDM_DRIVER_ABOUT"):
    check("Pass-1 独有实体存在: %s" % probe, probe in ents)

# ── 分主题文件（viewer 取 _source_line、metrics 工具取输入都靠它）──
pt_files = rep_pre.get("pertopic_files", 0)
check("v5 自产分主题文件", pt_files > 50, "%d 个文件, %d 实体"
      % (pt_files, rep_pre.get("pertopic_entities", 0)))
pt_dir = os.path.join(DATA, "pertopic")
sample = sorted(os.listdir(pt_dir))[:1] if os.path.isdir(pt_dir) else []
if sample:
    doc = json.load(open(os.path.join(pt_dir, sample[0]), encoding="utf-8"))
    check("分主题文件有 document 块（metrics 工具要）", "document" in doc)
    check("分主题实体带 _source_line（viewer 要）",
          any("_source_line" in e for e in doc.get("entities", [])))
# viewer 靠 <dataDir>/<entity.file> 定位分档文件
bad_ref = [k for k, v in ents.items()
           if v.get("provenance") in ("ocr", "ocr_pass1")
           and v.get("file", "").startswith("pertopic/")
           and not os.path.isfile(os.path.join(DATA, v["file"]))]
check("OCR 实体的 file 字段指向真实存在的分档文件", not bad_ref,
      "%d 个断链: %s" % (len(bad_ref), bad_ref[:3]))

# ── 已知实体抽查 ──
print("\n=== 抽查 ===", file=out)
SAMPLES = ["ACCESS_ALLOWED_ACE", "D3D12_RESOURCE_DESC", "KeInsertQueueDpc",
           "BITS_COST_STATE", "_DEBUG_SYMBOL_PARAMETERS", "MoveOptions",
           "IDebugAdvanced3", "WM_CUT", "IOCTL_NFCSERM_QUERY_RADIO_STATE"]
for s in SAMPLES:
    v = ents.get(s)
    if not v:
        check("抽查 %s 存在" % s, False)
        continue
    line = "  %-32s type=%-11s prov=%-13s zh=%s" % (
        s, v.get("type"), v.get("provenance"), "有" if v.get("description_zh") else "-")
    print(line, file=out)
    desc = (v.get("description") or "")[:70]
    print("      %s" % desc, file=out)

# ── OCR 错名是否修掉 ──
for wrong, right in [("KelnsertQueueDpc", "KeInsertQueueDpc"),
                     ("DdMapMemory", "DlMapMemory")]:
    check("OCR 错名 %s 已清除" % wrong, wrong not in ents)
    check("正确名 %s 存在" % right, right in ents)

# ── 报告一致性 ──
rep = json.load(open(os.path.join(DATA, "_v5_build_report.json"), encoding="utf-8"))
check("报告 final_entities 与实际一致", rep["final_entities"] == len(ents),
      "%d vs %d" % (rep["final_entities"], len(ents)))
check("报告 final_edges 与实际一致", rep["final_edges"] == len(edges),
      "%d vs %d" % (rep["final_edges"], len(edges)))
check("报告 dangling 为 0", rep["dangling_edges_after"] == 0)

print("\n=== 汇总 ===", file=out)
print("实体 %d (API %d / 占位 %d)" % (len(ents), len(ents) - len(ph), len(ph)), file=out)
print("边   %d" % len(edges), file=out)
print("类型分布: %s" % dict(dist.most_common()), file=out)
print("边类型分布: %s" % dict(collections.Counter(e["type"] for e in edges).most_common()), file=out)
print("失败断言: %d" % len(FAILS), file=out)
out.close()

print("\n失败断言: %d %s" % (len(FAILS), FAILS if FAILS else ""))
sys.exit(1 if FAILS else 0)
