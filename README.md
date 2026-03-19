# Windows API Knowledge Graph Pipeline

> 将 Windows API 文档（OCR 文本）自动转换为结构化知识图谱

## 项目概览

本项目从 OCR 扫描的 Windows API 文档出发，经过**规则提取 → 双源择优 → LLM 精炼 → 多轮增强**，
最终输出一个高密度、可查询的知识图谱，支持 Neo4j / Gephi / NetworkX 等工具消费。

### 当前规模（2026-03-19 基线）

| 指标 | 数值 |
|------|------|
| 文档对 | 70 |
| 实体总数 | ~19,000 |
| 边总数 | ~84,000+ |
| 实体类型 | function, structure, enum, callback, macro, typedef, ... |
| 边类型 | references, uses_type, parameter_type, return_type, belongs_to_header, ... |

## 目录结构

```
Windows_API_PDF_OCR_Graph/
├── pipeline.py               # 主入口：提取 + LLM 精炼
├── pipeline_lib/             # 共享配置包
│   ├── __init__.py
│   └── config.py             # 常量、类型白名单、normalize_entity_type()
│
├── scripts/                  # 增强/维护/导出脚本
│   ├── kg_enrich_v41.py      # v4.1: 签名自动建边 + 类型清洗
│   ├── kg_enrich_v43.py      # v4.3: enum值/类型别名/callback/深度类型
│   ├── kg_connect_isolated_v42.py  # v4.2: 启发式孤立节点连接
│   ├── enrich_from_syntax.py # 从 syntax 回填参数类型 + header/domain边
│   ├── embed_link_isolated.py# Embedding 语义兜底
│   ├── fix_entity_types.py   # 一次性 entity_type 清洗
│   ├── export_graph.py       # Neo4j / GraphML / GEXF 导出
│   ├── assess_isolated_nodes.py  # 孤立节点评估
│   ├── evaluate_graph_metrics.py # 综合图指标
│   ├── analysis_improvement_options.py # 改进方案分析
│   └── RESOURCE_INDEX.py     # 资源索引生成
│
├── OCR_raw/                  # 输入：OCR 文本（.p.txt + .txt 双源）
├── json_output_v4/           # 输出：实体 JSON + 全局索引/边 + 报告
├── exports/                  # 导出：Neo4j CSV / GraphML / GEXF
├── tests/                    # pytest 单元测试
│   ├── test_config.py        # 类型归一化测试
│   ├── test_extraction.py    # 提取逻辑测试
│   └── test_export.py        # 导出功能测试
├── gt_templates/             # Ground Truth 评估模板
│
├── requirements.txt          # Python 依赖
├── llm_config_example.json   # LLM 配置模板
├── entity_aliases.json       # 实体别名表
├── AGENTS.md                 # 项目约定与架构指南
└── .gitignore
```

## 快速开始

### 1. 环境准备

```bash
pip install -r requirements.txt
# sentence-transformers 仅在使用 embedding 语义连接时需要
```

### 2. 配置 LLM（可选，仅精炼阶段需要）

```bash
cp llm_config_example.json llm_config.json
# 编辑 llm_config.json，填入 API key 和模型名
```

### 3. 运行主管线

```bash
# 完整流程（提取 + 精炼）
python pipeline.py

# 仅提取（不需要 LLM）
python pipeline.py --phase extract

# 仅精炼（从断点续跑）
python pipeline.py --phase refine --resume

# 预览模式（不调用 LLM）
python pipeline.py --dry-run
```

### 4. 运行增强脚本

增强脚本按依赖顺序执行：

```bash
# Step 1: 从 syntax 回填参数类型 + 添加 header/domain 边
python scripts/enrich_from_syntax.py --apply

# Step 2: v4.1 签名建边
python scripts/kg_enrich_v41.py

# Step 3: v4.3 深度类型增强
python scripts/kg_enrich_v43.py --strategy all --apply

# Step 4: Embedding 语义兜底（需要 sentence-transformers）
python scripts/embed_link_isolated.py --apply
```

### 5. 导出

```bash
python scripts/export_graph.py --format all
# 输出到 exports/neo4j/、exports/graphml/、exports/gexf/
```

### 6. 评估

```bash
python scripts/assess_isolated_nodes.py
```

### 7. 图谱查看器（graph_viewer）

- **前端画布库（随仓库提供）**：`force-graph`、`three`、`3d-force-graph` 的 min 文件已放在 `graph_viewer/static/`，由本机 `go embed` 一并提供，**不再依赖 unpkg CDN**，避免国内网络下脚本加载失败导致一直转圈。若需升级版本，可覆盖对应 `.min.js` 后重新 `go run` / 编译。
- **2D/3D 视图切换**：右上角可切换 2D/3D 视图模式。2D 视图使用 Canvas 渲染，3D 视图使用 WebGL 渲染。
- **边箭头显示**：
  - 2D 和 3D 视图均支持有向边的箭头显示
  - 箭头方向表示 caller → callee 的关系
  - 侧栏提供「Arrow size」滑块（20% - 150%），默认 67%，可调整箭头大小
  - 箭头在视觉上保持固定大小，不会随视图缩放而变化
  - 某些边类型（如 `semantically_related`、`via:*`）不显示箭头
- 侧栏 **「树形根视图」**：开启后点击节点，将以该点为根做 BFS 分层树形排布（边集不变），节点沿**三次贝塞尔曲线**过渡到新位置；根节点仍用右侧浮窗详情，**1～3 层**邻居在画布上显示名称标签（2D 为画布文字，3D 为精灵标签，需加载 Three.js）。
- 节点详情浮窗：**OCR 原始文本**区会显示 `_source_line` 行号、原文上下行摘录，并提供 `[OCR]_windows-*.p.txt` / `.txt` 整文件下载。默认 OCR 目录为数据目录上一级的 `OCR_raw/`；若不在该位置，请 `go run . --data ... --ocr /path/to/OCR_raw`。
- 全局索引需含 `description` 字段，详情页才有摘要（见下节）。
- **外观主题**：侧栏「外观主题」可选 **暗黑 / 明亮 / 跟随系统**（`prefers-color-scheme`），选择会写入浏览器 `localStorage`（`graph-viewer-theme`），首屏前有内联脚本减轻闪烁；画布背景在明亮模式下与 UI 一致（浅色底），暗色下仍使用当前视图方案的预设背景色。
- **缓存控制**：服务器已配置 `Cache-Control: no-cache` 头，确保浏览器始终加载最新的 JavaScript 代码，避免缓存导致的功能问题。
- **磁盘热更新（默认关闭）**：需要监测磁盘变更时加 `--auto-reload`（默认每 2 秒检查 `global_entity_index.json` / `global_edges.json`）；否则为「笨模式」——数据更新后请**重启 graph_viewer** 并刷新浏览器。开启时可调 `--reload-interval=3s`。网页「Save」后约 4 秒内会跳过自动重载。外部重载会清空本次会话的撤销/重做栈。
- **页面定时拉取（默认关闭）**：前端不再每隔数秒请求 `/api/graph`；要看最新图请**手动刷新浏览器**（Undo/Redo、保存等仍会走接口更新）。

### 8. MCP 服务器与 LLM 对话框

- **MCP 服务器**：将图谱 API 暴露给大模型（如 Cursor），支持轮询节点、验证关系建立。需先启动 graph_viewer，再运行 MCP 服务：

```bash
./start_graph_viewer.sh   # 或 go run . --data json_output_v4
cd graph_viewer && go run ./mcp_server --graph-url=http://localhost:10086
```

  在 Cursor 等支持 MCP 的客户端中配置该服务器后，可使用工具：`kg_get_graph`、`kg_get_node`、`kg_search`、`kg_get_node_context`、`kg_evaluate`。

- **LLM 对话框**：点击节点后，左下角会浮现「AI 解读」面板，展示该节点的完整上下文（类型、边、邻居、OCR 原文），供大模型做解读或生成示例代码。该面板位于左下角，不遮挡右侧节点详情浮窗。

### 9. 全局索引描述（graph_viewer）

`global_entity_index.json` 需包含 `description` 字段，查看器节点详情才会显示文档摘要。新版本 pipeline 已自动写入；若你仍在使用旧索引，可一次性回填：

```bash
python scripts/enrich_index_descriptions.py --apply
./start_graph_viewer.sh
```

## 数据流水线

```
OCR_raw/*.txt
     │
     ▼
pipeline.py (extract)          # Phase 0-2: 发现→双趟提取→择优
     │
     ▼
json_output_v4/*.json          # 每文档实体 JSON
     │
     ▼
pipeline.py (refine)           # Phase 3: LLM 逐实体精炼
     │
     ▼
json_output_v4/global_*.json   # 全局索引 + 边
     │
     ├──→ enrich_from_syntax   # 回填类型、添加 header/domain 边
     ├──→ kg_enrich_v41        # 签名自动建边
     ├──→ kg_enrich_v43        # 类型别名/enum/callback/深度类型
     ├──→ embed_link_isolated  # Embedding 语义兜底
     │
     ▼
exports/                       # Neo4j / GraphML / GEXF
```

## 核心设计原则

1. **先提取，后精炼** — 不让 LLM 从零生成，而是在稳定基线上做增量修正
2. **双源择优** — .p.txt 与 .txt 双路提取并打分，减少 OCR 噪声
3. **类型归一化前置** — `entity_type` 入图前必须通过 `normalize_entity_type()` 归一
4. **LLM 只输出操作指令** — `update_field` / `add_edge` / `delete_edge` / `add_node` / `delete_node` / `merge_into`，由执行器统一落地
5. **全流程可恢复** — 提取断点 + 精炼断点 + 操作日志，支持中断续跑

## 断点与恢复

| 文件 | 用途 |
|------|------|
| `_checkpoint.json` | 提取阶段断点 |
| `_llm_checkpoint.json` | 精炼阶段断点 |
| `_llm_operations.jsonl` | LLM 操作日志（可回放） |
| `_extraction_report.json` | 提取统计报告 |
| `_refinement_report.json` | 精炼统计报告 |

恢复精炼：
```bash
python pipeline.py --phase refine --resume
```

从头重跑精炼（先备份）：
```bash
cp json_output_v4/_llm_checkpoint.json json_output_v4/_llm_checkpoint.json.bak
python pipeline.py --phase refine
```

## 边类型分层

| 层级 | 边类型 | 来源 | 语义强度 |
|------|--------|------|----------|
| 显式引用 | `references` | 文本交叉引用 | 强 |
| 签名推断 | `parameter_type`, `return_type`, `uses_type` | 函数签名解析 | 强 |
| 结构关系 | `belongs_to`, `member_of`, `contains` | LLM 精炼 | 强 |
| 类型别名 | `type_alias_of` | 指针前缀推断 | 中 |
| 层次归属 | `belongs_to_header`, `belongs_to_domain` | 文件/头文件解析 | 弱 |
| 语义兜底 | `semantically_related` | Embedding 相似度 | 弱 |

## 测试

```bash
python -m pytest tests/ -v
```

覆盖范围：类型归一化、OCR 纠错、参数解析、签名解析、导出格式。

## 依赖

- Python 3.10+
- `aiohttp >= 3.9` — 异步 LLM 调用
- `tqdm >= 4.66` — 进度条
- `networkx >= 3.2` — 图导出
- `sentence-transformers >= 3.0`（可选）— Embedding 语义连接

## 故障排查

| 问题 | 排查 |
|------|------|
| 提取 0 对 | 检查 `OCR_raw/` 下是否有 `[OCR]_*.txt` |
| 精炼太慢 | `--dry-run` 估算规模，`--max-entities 3000` 分批跑 |
| API 限流 | 降低 `llm_config.json` 中 `requests_per_min` |
| 断点异常 | 备份后删除 `_llm_checkpoint.json` 重跑 |

## 许可

内部使用。
