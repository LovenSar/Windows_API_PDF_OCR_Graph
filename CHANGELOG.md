# Changelog

## 0.2.0 - 2026-09-21

**并集库 v5：补回 0.1.2 丢掉的 Win32 桌面覆盖面，修掉 0.1.2 的类型塌陷。**

0.1.2 把输出从 OCR 换成了 Markdown，但那份 Markdown 覆盖的是另一批 API。实测：
`json_output_v4_v020_markdown` 的 23,261 个实体里 **20,932 个（90%）来自 Driver DDI**，
只有 2,329 个来自 desktop-src；而本地 desktop-src 只有 124 个 `nf-` 文件，几乎没有
Win32 桌面 API 参考页（`AbortDoc` / `ActivateKeyboardLayout` / `ACCESS_ALLOWED_ACE`
逐个 `find` 验证过，确实不存在）。两份产物同名交集只有 2,605（11%），是两个基本
不相交的 API 宇宙。所以 0.1.2 不是「更干净」，是换了覆盖面。

v5 由 `scripts/build_v5.py` 从**三个来源**合成 `json_output_v5/`：

### 一、新增 sdk-api 源（补 Win32 桌面 API 缺口）

本地 desktop-src 是解压快照而非完整 checkout，Win32 桌面 API 参考页基本没有。
补拉 `MicrosoftDocs/sdk-api`（`docs` 分支，稀疏检出 `sdk-api-src/content`，65,908 个 .md）：

- **抽取器名字塌陷修复**（`scripts/markdown_to_entities.py`）：COM 方法页的 `api_name`
  常只写接口名，按 `id` 去重时各接口下的同名方法（`Delete` / `Copy` / `GetCount`）
  会全塌成一个实体。改用 `UID`（形如 `NF:propidl.IPropertySetStorage.Delete`）取限定名。
  实测 sdk-api 59,420 个 API 页里 **29,484 个**属这种情况。
  修复前后：**35,802 → 64,629 个实体**。
- **补参数与返回类型解析**：sdk-api 页没有 `## Syntax`，签名在 `### -param x [in]`
  与 `## -returns` 的 `Type: <b>XXX</b>` 行里。原来的正则要求 `### -param <name>`
  行尾即结束，`[in]` 标注导致参数全漏。
  修复前后：`parameter_type` 边 **5 → 12,509**，`return_type` 边 **2 → 6,800**。

### 二、类型与垃圾治理

- **类型塌陷修复**：`markdown_to_entities.py` 把 `entity_type` 默认写死成 `"function"`，
  而源 front matter 里基本没有可用的 `topic_type`/`api_type`，导致实体被压成 function。
  改为按文件名前缀重定。前缀表是读样本确认的，不是照 MS Docs 命名习惯猜的：
  - 常见误判已修正：`ni-` 不是 interface 而是 **IOCTL**；`nn-` 才是 interface；
    `nl-` 是 class
  - `ip-` / `rc-` / `cd-` / `vs-` / `mf-` / `em-` / `ec-` / `tb-` 等是**假前缀**
    （`ip-address-controls.md`、`rc-diagnostic-messages.md` 会误命中），一律不认
  - 源文件 front matter 的 `title` 作为权威类型声明，与前缀推导冲突时以 title 为准
- **垃圾页剔除 2,936 条**：门户页（`Learn more about:` 文案）、概念页标题
  （`Registry` / `Overview` 这类从 title 首词猜出来的）、列表页、C 关键字。
  判据只用能确证的强信号——**没有**采用「有 `## Syntax` 就保留」，因为实测
  无前缀文件里只有 16.4% 带该标记。

### 三、OCR 侧富字段回收与方法名归属解析

- **分文档 JSON 里的字段被汇总索引丢了**：`json_output_v4/global_entity_index.json`
  每个实体只有 5 个字段，而 70 个分文档 JSON 里有 `header`（18,166/18,927）、
  `confidence`（真实值，分布 0.5–0.85）、`syntax`、`parameters`（带类型与方向）、
  `return_value`、`cross_references`、`_source_line`。全部回收到 v5。
  其中 `cross_references` 补出 **24,615 条 references 边**。
- **528 个裸方法名归属接口解析，457 个成功（87%）**，证据按可靠性排序：
  | 依据 | 条数 |
  |------|------|
  | `header` + 方法名匹配 sdk-api 目录 | 408 |
  | 全局方法名候选唯一 | 27 |
  | `_source_line` 附近窗口定位接口 | 13 |
  | `header` + 描述/原文点名接口收敛 | 3 |
  | 描述/原文全文收敛 | 6 |
  另有 **23 个查明根本不是方法**（描述证据：属性 11 / 全部大写常量 7 / 结构体 5），
  重定类型。剩 **48 个真歧义**（`GetName` 这类在几十个接口上都存在），
  保持裸名并打 `name_qualified: false`，不猜。
- **只做能双向确认的名字修正 3 条**：`KelnsertQueueDpc` → `KeInsertQueueDpc`、
  `DdMapMemory` → `DlMapMemory`、`IoUnRegisterBootDriverCallback` →
  `IoUnregisterBootDriverCallback`。**不做**编辑距离猜测：实测 1 字符差的配对里有
  `PDEVICE_OBJECT` / `DEVICE_OBJECT`、`KSJACK_DESCRIPTION2` / `KSJACK_DESCRIPTION`
  这类本来就不同的真实符号，判据分不开。
- **HRESULT 类符号改型**：117 个（`AUDCLNT_E_*` / `STG_E_*` / `TBS_E_*`）
  原本被标成 `method`/`structure`/`macro`，改回 `error_code`。

### 四、边与字段契约

- **边治理**：丢弃 embedding/启发式兜底的 `semantically_related` 与 `related`；
  去自环、去重；丢源实体已剔除的边；丢目标落空的 `references`（markdown 的
  See-also 目标大量是链接文字，如 `'Direct3D 12 Reference'`，不是 API 名）。
  目标天然是另一类东西的边补独立命名空间占位节点：`type::` 3,086、`header::` 1,581、
  `domain::` 77，**不混进 `windows::` 实体表**。最终悬空边 0。
- **中文描述副字段**：同名交集实体挂上 OCR 侧的 LLM 中文摘要 `description_zh`，
  英文原文 `description` 保持权威不动。用 `provenance`
  （`markdown` / `ocr` / `markdown+ocr` / `placeholder`）+ `name_verified` +
  `source_repo` 表达来源，而不是编造一个置信度数字。

效果：

| 指标 | 0.1.2 (v020) | v5 |
|------|--------------|-----|
| 实体 | 23,261（90% 是 DDI） | **100,677**（API 实体 95,933 + 占位 4,744） |
| 边 | 45,777 | **182,826** |
| 悬空边 | 41,698（91%） | **0** |
| entity_type distinct | 2（function 23,251 + macro 10） | **27** |
| unknown 占比 | 0（但 97% 被误标 function） | 0.12% |

**已知局限**（不粉饰）：

- `graph_viewer/main.go:34` 读的 json 标签是 `type`，而 v020 只写 `entity_type`，
  所以 0.1.2 的产物在查看器里类型全是空的。v5 两个字段都写。
- 48 个 OCR 裸方法名归属接口定不了，跨接口可能撞名，已打 `name_qualified: false`。
- 2 个名字确认被截断（`GetSpatialAudioMetadataItemsBufferLengt`、
  `CfGetWin32HandleFromProtectedHandl`）。另有 52 个 HRESULT 风格名字打了
  `name_suspect`：这个标记**精度低**——多数是真错误码（`APPX_E_DIGEST_MISMATCH`）
  只是描述里没提「错误」二字，少数才是截断名（如 `E_POLICY` 实为
  `PROCESS_MITIGATION_*_POLICY`）。一律**未猜原名**。
- OCR 侧部分实体的描述是分片错误带过来的（如 `DISPATCH_LEVEL` 的描述在讲自旋锁），
  无自动判据可修，只能靠类型重定缓解。
- 新增 `scripts/verify_v5.py`，34 条断言全部通过（结构契约、悬空、自环、去重、
  类型分布、占位命名空间越界、Win32 补全、方法名解析率）。

### 五、上游中间产物清理（约 650MB）

建成后清理已被取代的中间产物：`json_output_v4` 184M → 33M，`json_output_v4_v020_markdown`
192M → 39M。

**关键发现：Pass-1 不能直接删。** 70 个「择优」分档文件是 Pass-1 的**真子集**——
`pipeline.py` 的择优那步只选了一路（`.p.txt` 或 `.txt`），另一路的实体整个没进后续
流程。实测 Pass-1 独有 4,679 个实体名，其中 **3,894 个连 sdk-api / DDI 都没有**，
多是 Win32 常量、标志位、枚举值（`AF_INET`、`ACM_DRIVERADDF_*`、`AD_CLOCKWISE`
这些 sdk-api 不建独立页面的符号）。

处理方式：先并入（`provenance: ocr_pass1`，**5,063 个**），再由 v5 自产同形状的
`pertopic/`（62 个文件 / 14,698 实体），才清理原始 Pass-1。

同时修了两个消费方的输入源，它们原来都吃 OCR 的 `_p_*.json`：

- `scripts/evaluate_graph_metrics.py:2146` 默认 pattern 与 `--output` 改指 v5
- `scripts/assess_isolated_nodes.py:18` 改读 `json_output_v5/pertopic/`；
  注意 `OUT_DIR` 必须留在 v5 根目录，它还要读 `<OUT_DIR>/global_edges.json` 补边，
  指到 `pertopic/` 子目录会让孤立率虚高到 83%（实际 8.02%）
- `graph_viewer/main.go:111` 的 `readEntitySourceLine` 按 `<dataDir>/<entity.file>`
  定位分档文件，v5 现在把 OCR 实体的 `file` 指向 `pertopic/_v5_<topic>.json`，
  查看器的 OCR 原文功能对 v5 才真正可用

**重建代价**：`build_v5.py --pass1-dir` 现在读不到东西，重跑得到 100,677 实体
而非 104,846。完整重建需先 `python pipeline.py --phase extract` 重新生成 Pass-1。

**最终规模**：104,846 实体（API 100,210 + 占位 4,636）/ 197,364 边 / 悬空 0。

**范围**：本轮变更全部落在 root 组件 `winapi_graph`（0.1.2 → 0.2.0），未触及
`docs/` 或 `tools/` 组件。`json_output_v4/` 与 `json_output_v4_v020_markdown/`
只读不改，作为 v5 的上游输入保留。sdk-api 与 DDI 是外部 clone，不在本仓内。

## 0.1.2 - 2026-09-21

**重大更新：从 OCR 切换到 Microsoft 官方 Markdown 数据源。**

- **数据源升级**：从 `OCR_raw/` (2026-03-05 OCR'd PDF，6.5 个月前) 切换到 MicrosoftDocs 官方 GitHub 仓库的 Markdown 源：
  - `MicrosoftDocs/win32` (docs 分支, 236 MB / 48,213 个 .md, `desktop-src/<topic>/...`)
  - `MicrosoftDocs/windows-driver-docs-ddi` (staging 分支, 35.8 MB / 25,913 个 .md, `wdk-ddi-src/content/...`)
- **新增 extractor**：`scripts/markdown_to_entities.py` 解析 YAML front matter (api_name / api_type / api_location / req.header) + Markdown body (## Syntax / ## -parameters / ## See also / ## -see-also),支持：
  - 兼容 Win32 (desktop-src) 与 Driver DDI (wdk-ddi-src) 两种 Markdown 风格
  - SAL 注解清理 (`_In_` / `_Inout_` → 剥掉)
  - C 函数签名 + 宏定义 (`#define NAME(args)`) 双解析
  - api_name 多版本 (A/W 变体) 产生 alias entities
  - URL 反向解析 API 名 (nf-/ns-/ne- 前缀)
  - 相对路径解析 (`./nf-...md`) 用于 DDI see-also
- **新增 batch + merge 脚本**：
  - `scripts/markdown_batch.py` — 递归处理整个 markdown 目录
  - `scripts/merge_v020.py` — 合并 desktop-src + driver-docs-ddi 抽取产物 (冲突时优先字段多的)
- **新增测试**：20 个 case / 8 个 class (YAML/SAL/签名/宏/See also/端到端)
- **效果对比**：
  - entities: 18,534 → **23,261** (+26%)
  - edges: 71,694 → **45,777** (-36%，更紧密 schema)
  - 边类型: 11 → **4**（更纯 schema: parameter_type / references / return_type / belongs_to_header）
  - 全部 entity 来源 confidence=1.0（vs OCR 的 ~67% 低置信度）
  - 0 unknown / 0 no-desc / 0 short-desc
- **OCRmyPDF**：v17.12.1 已确认最新 release，但本轮不需要（全部 Markdown 数据已能完整覆盖 Win32 API + Driver DDI）。
- **OCR_raw 仍保留**：作为 PDF 备份源 / 历史参考 / 与 v0.1.x OCR 模式对比。

## 0.1.1 - 2026-09-21

Offline fix for two HIGH-severity extraction bugs in `global_entity_index.json` / `global_edges.json`:

- **§3.1 HIGH — entity key casing collision.** Added `normalize_entity_key()` and `merge_entity_keys_case_insensitive()` in `pipeline_lib/config.py`; new offline script `scripts/fix_entity_key_casing.py` deduplicates 19 case-collision groups (incl. the canonical `EvtSerCx2SetWaitMask` vs `EvtSerCx2SetWaitmask` bug). Merged 19,927 → 19,907 entities. PowerShell `ConvertFrom-Json` now successfully parses the entity index (previously threw on duplicate-key detection).
- **§3.2 HIGH — edge type vocabulary explosion.** Added `ALLOWED_EDGE_TYPES` (11 canonical: `references` / `uses_type` / `parameter_type` / `return_type` / `belongs_to` / `member_of` / `contains` / `belongs_to_header` / `belongs_to_domain` / `semantically_related` / `related`) and `normalize_edge_type()` with 204 explicit synonym mappings plus 9-tier fuzzy fallback. New offline script `scripts/fix_edge_types.py` collapses 212 distinct edge types down to the 11-element whitelist; 91,204 → 87,656 edges after de-dup of same `(source, target, type)` triples.
- **Test coverage:** 36 new unit tests in `tests/test_normalize_edge_type.py` (22 cases) and `tests/test_normalize_entity_key.py` (14 cases). All 49 collected tests / 200 subtests pass; `agents_doccheck` exit 0.
- **Scope:** bug fixes only. PDF re-fetch from MS Learn, OCR pipeline, and full pipeline re-run are deferred to 0.2.0 (see plan in commit scope).

## 0.1.0 - 2026-09-02

Governance baseline. Adds schema_version 1 AGENTS fact tables and the satellite `agents_doccheck` family. `0.1.0` does not mean first implementation or first graph export.
