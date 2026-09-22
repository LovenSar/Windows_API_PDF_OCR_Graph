# AGENTS.md — Windows API document knowledge graph

## 组件事实

| 字段 | 值 |
|------|-----|
| schema_version | 1 |
| component_id | winapi_graph |
| internal_version | 0.2.0 |
| updated | 2026-09-21 |
| owner | P4 |
| role | Windows API 文档图谱：实体/边文件供 Detection Surface 导入，不进入 start_all |
| core_files | AGENTS.md, example.graphy.json, pipeline.py, pipeline_lib/config.py, scripts/build_v5.py, scripts/verify_v5.py, scripts/fix_entity_key_casing.py, scripts/fix_edge_types.py, scripts/markdown_to_entities.py, scripts/markdown_batch.py, scripts/merge_v020.py, scripts/improve_entity_quality.py, docs/DEVELOPMENT_RULES.md, tools/agents_doccheck/check.py |
| public_entry | example.graphy.json |
| upstream | - |
| downstream | ttps_knowledge, llm_aav |
| config_entry | example.graphy.json |
| outputs | json_output_v5/ |

## 公开接口

| 接口 | 类型 | 稳定性 | owner 路径 |
|------|------|--------|------------|
| graph file_format | schema | public | example.graphy.json |
| v5 并集库 schema | schema | public | json_output_v5/global_entity_index.json |
| build_v5（重建图谱） | cli | public | scripts/build_v5.py |
| verify_v5（输出契约校验） | cli | public | scripts/verify_v5.py |
| evaluate_graph_metrics | cli | public | scripts/evaluate_graph_metrics.py |

> v5 并集库 schema 新增字段：`provenance`（`markdown` / `ocr` / `markdown+ocr` / `placeholder`）、
> `name_verified`、`description_zh`、`name_qualified`、`name_suspect`，以及三类占位命名空间
> （`type::` / `header::` / `domain::`）。`type` 与 `entity_type` 同值双写。

> 所属方向: P4 — 知识图谱卫星仓

> 本仓文档依赖传播协议见 [docs/DEVELOPMENT_RULES.md](docs/DEVELOPMENT_RULES.md)，不要在此复制。
> 文档治理失败等同于实现失败。不得以代码已通过测试为由跳过文档、版本、接口或传播检查。

任意功能修改必须首先确定该功能的唯一所属组件。修改完成后更新该组件当前状态，并沿目录层级向本仓根做影响检查。没有影响就不更新上层正文，但必须完成影响检查。叶子版本不传播到 Hub。Hub 通过 `GRAPH_ROOT` 指向本仓根；本仓不接入 `start_all`。`json_output_v4/`、`json_output_v4_v020_markdown/`、`json_output_v5/` 继续 gitignore，不是提交集。产品导入只消费 `example.graphy.json` 的 `file_format`（`global_entity_index.json` + `global_edges.json`）。`graph_viewer` 仍是本仓独立工具，不嵌入产品 UI。

**v5 是当前产物**（`json_output_v5/`，2026-09-21）。它是三源并集库：sdk-api（Win32 桌面 API 权威源）+ win32/DDI（以 Driver DDI 为主）+ OCR/PDF。`json_output_v4/` 与 `json_output_v4_v020_markdown/` 只读保留作为上游输入，不要就地修改。

**上游源位置**（sdk-api 与 DDI 是外部 clone，不在本仓内；路径在 `scripts/build_v5.py` 顶部有默认值，可用参数覆盖）：

| 源 | 路径 | 说明 |
|----|------|------|
| sdk-api 抽取产物 | `D:\KnowLedgeBase\_raw\Tools\MS_Docs_SdkApi\sdkapi_entities.json` | `build_v5.py --sdkapi` |
| sdk-api 源目录 | `D:\KnowLedgeBase\_raw\Tools\MS_Docs_SdkApi\sdk-api\sdk-api-src\content` | 方法名索引用，`--sdkapi-src` |
| DDI 源目录 | `D:\KnowLedgeBase\_raw\Tools\MS_Docs_DriverDDI\windows-driver-docs-ddi-staging\wdk-ddi-src\content` | 方法名索引用，`--ddi-src` |
| OCR 原文 | 本仓 `OCR_raw/` | 丢接口限定的方法回原文找上下文，`--ocr-root` |

重新拉 sdk-api（Windows 需先开长路径，仓库里有超长文件名）：
`git config --global core.longpaths true && git clone --depth 1 --filter=blob:none --sparse --branch docs https://github.com/MicrosoftDocs/sdk-api.git sdk-api && cd sdk-api && git sparse-checkout set --no-cone "sdk-api-src/content/*"`

## 目录索引

| component_id | 路径 | 职责 | 导航 |
|--------------|------|------|------|
| docs | `docs/` | 本仓文档依赖传播协议 | [AGENTS.md](docs/AGENTS.md) |
| tools | `tools/` | 本仓 agents_doccheck | [AGENTS.md](tools/AGENTS.md) |

## 目录结构

```
Windows_API_PDF_OCR_Graph/
├── pipeline.py               # extract + LLM refine
├── pipeline_lib/config.py    # type whitelist, normalize_entity_type()
├── scripts/                  # enrich / export / evaluate_graph_metrics.py
├── graph_viewer/             # standalone viewer + MCP (not product UI)
├── OCR_raw/                  # OCR text inputs
├── json_output_v5/           # gitignored 当前产物：并集库
├── json_output_v4/           # gitignored 上游输入（OCR 源）
├── json_output_v4_v020_markdown/  # gitignored 上游输入（Markdown 源）
├── exports/                  # gitignored Neo4j / GraphML / GEXF
├── tests/                    # pytest
├── example.graphy.json       # node/edge file_format contract
└── tools/agents_doccheck/    # satellite document checker
```

## 构建与测试

```bash
python tools/agents_doccheck/check.py
python -m unittest discover -s tools/agents_doccheck -p '*_test.py'
python -m pytest tests/ -v
```

## 关键约定

All `entity_type` writes go through `pipeline_lib.config.normalize_entity_type()`. Do not bypass that helper in LLM executors or exporters.

Normalization: `struct`/`structur` → `structure`; `flag` → `flags`; `enumvalue` → `enum_value`; length > 40 or not in `ALLOWED_ENTITY_TYPES` → `unknown`.

Edge layers: strong (`references`, `uses_type`, `parameter_type`, `return_type`); structural (`belongs_to`, `member_of`, `contains`); header/domain (`belongs_to_header`, `belongs_to_domain`); weak (`semantically_related`). Isolated-node rates must keep those layers distinct.

`json_output_v4/` naming: `_p_*.json` Pass-1 from `.p.txt`; `_t_*.json` Pass-1 from `.txt`; unprefixed domain JSON after refine; `global_*.json` index and edges. Checkpoints: `_checkpoint.json`, `_llm_checkpoint.json`, `_llm_operations.jsonl`. Resume with `python pipeline.py --phase refine --resume`. `json_output_v4/`、`json_output_v4_v020_markdown/`、`json_output_v5/` 和 `exports/` stay untracked.

**上游中间产物已清理**（2026-09-21，约 650MB）。其中 Pass-1（`json_output_v4/_p_*.json` / `_t_*.json`）不能直接删——70 个择优分档是它的真子集，择优丢掉的另一路实体只在那里。已先把 5,063 个 Pass-1 独有实体并入 v5 再删。代价：`build_v5.py --pass1-dir` 现在读不到东西，重跑得到 100,677 实体而非 104,846；完整重建需先 `python pipeline.py --phase extract`。

`json_output_v5/pertopic/`（62 个文件 / 14,698 实体）是 v5 自产的分主题文件，形状对齐原来的 Pass-1（带 `document` 块 + 每实体 `_source_line` / `cross_references` / `confidence`）。`graph_viewer` 取 `_source_line`、`scripts/evaluate_graph_metrics.py` 与 `scripts/assess_isolated_nodes.py` 取输入都读它，不再依赖 OCR 原始产物。

v5 实体字段约定：`type` 与 `entity_type` 同值双写（`graph_viewer/main.go:34` 读 json 标签 `type`，pipeline 侧消费 `entity_type`）。类型来源优先级：源文件 front matter 的 `title` > 文件名前缀（`nf`/`ns`/`nc`/`ne`/`ni`/`nn`/`nl`/`wm`/`mm`/`nt`，**不含** `ip-`/`rc-`/`cd-` 这类假前缀）> 描述形态。OCR 侧**不编造 `confidence`**，用 `provenance`（`markdown` / `ocr` / `markdown+ocr` / `placeholder`）+ `name_verified` 表达来源与可信度。占位节点一律独立命名空间（`type::` / `header::` / `domain::`），不得混进 `windows::`。重建与验证：`python scripts/build_v5.py && python scripts/verify_v5.py`。

## 常用命令

```bash
python pipeline.py
python pipeline.py --phase extract
python pipeline.py --phase refine --resume
python scripts/build_v5.py
python scripts/verify_v5.py
python scripts/evaluate_graph_metrics.py
python -m pytest tests/ -v
```

重抽 sdk-api（改了 `markdown_to_entities.py` 的抽取逻辑后需要）：

```bash
MARKDOWN_BATCH_ALL_MD=1 python scripts/markdown_batch.py \
  --desktop-src "$SDKAPI_SRC" --ocr-root OCR_raw \
  --output "D:/KnowLedgeBase/_raw/Tools/MS_Docs_SdkApi/sdkapi_entities.json"
```

`graph_viewer` stays a standalone tool (`./start_graph_viewer.sh` or `cd graph_viewer && go run . --data ../json_output_v5`). Do not embed it in the product UI.
