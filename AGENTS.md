# AGENTS.md — Windows API document knowledge graph

## 组件事实

| 字段 | 值 |
|------|-----|
| schema_version | 1 |
| component_id | winapi_graph |
| internal_version | 0.1.0 |
| updated | 2026-09-02 |
| owner | P4 |
| role | Windows API OCR 文档图谱：实体/边文件供 Detection Surface 导入，不进入 start_all |
| core_files | AGENTS.md, example.graphy.json, pipeline.py, docs/DEVELOPMENT_RULES.md, tools/agents_doccheck/check.py |
| public_entry | example.graphy.json |
| upstream | - |
| downstream | ttps_knowledge, llm_aav |
| config_entry | example.graphy.json |
| outputs | json_output_v4/ |

## 公开接口

| 接口 | 类型 | 稳定性 | owner 路径 |
|------|------|--------|------------|
| graph file_format | schema | public | example.graphy.json |
| evaluate_graph_metrics | cli | public | scripts/evaluate_graph_metrics.py |

> 所属方向: P4 — 知识图谱卫星仓

> 本仓文档依赖传播协议见 [docs/DEVELOPMENT_RULES.md](docs/DEVELOPMENT_RULES.md)，不要在此复制。
> 文档治理失败等同于实现失败。不得以代码已通过测试为由跳过文档、版本、接口或传播检查。

任意功能修改必须首先确定该功能的唯一所属组件。修改完成后更新该组件当前状态，并沿目录层级向本仓根做影响检查。没有影响就不更新上层正文，但必须完成影响检查。叶子版本不传播到 Hub。Hub 通过 `GRAPH_ROOT` 指向本仓根；本仓不接入 `start_all`。`json_output_v4/` 继续 gitignore，不是提交集。产品导入只消费 `example.graphy.json` 的 `file_format`（`global_entity_index.json` + `global_edges.json`）。`graph_viewer` 仍是本仓独立工具，不嵌入产品 UI。

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
├── json_output_v4/           # gitignored entity index, edges, reports
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

`json_output_v4/` naming: `_p_*.json` Pass-1 from `.p.txt`; `_t_*.json` Pass-1 from `.txt`; unprefixed domain JSON after refine; `global_*.json` index and edges. Checkpoints: `_checkpoint.json`, `_llm_checkpoint.json`, `_llm_operations.jsonl`. Resume with `python pipeline.py --phase refine --resume`. `json_output_v4/` and `exports/` stay untracked.

## 常用命令

```bash
python pipeline.py
python pipeline.py --phase extract
python pipeline.py --phase refine --resume
python scripts/evaluate_graph_metrics.py
python -m pytest tests/ -v
```

`graph_viewer` stays a standalone tool (`./start_graph_viewer.sh` or `cd graph_viewer && go run . --data ../json_output_v4`). Do not embed it in the product UI.
