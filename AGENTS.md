# AGENTS.md — 项目约定与架构指南

## 项目定位

将 Windows API 文档（OCR 文本）自动转换为**结构化知识图谱**，
用于 API 检索/问答、语义关联分析、代码生成上下文。

## 目录结构

```
Windows_API_PDF_OCR_Graph/
│
├── pipeline.py               # 主入口 — 提取 + LLM 精炼（~2000 行）
├── pipeline_lib/             # 共享配置包
│   ├── __init__.py
│   └── config.py             # 常量、类型白名单、normalize_entity_type()
│
├── scripts/                  # 增强/维护脚本（按版本编号）
│   ├── kg_enrich_v41.py      # v4.1: 类型清洗 + 签名自动建边
│   ├── kg_enrich_v43.py      # v4.3: enum 值/类型别名/callback/深度类型
│   ├── kg_connect_isolated_v42.py  # v4.2: 孤立节点启发式连接
│   ├── enrich_from_syntax.py # 从 syntax 回填参数类型 + header/domain 边
│   ├── embed_link_isolated.py# Embedding 语义兜底
│   ├── fix_entity_types.py   # 一次性 entity_type 清洗
│   ├── export_graph.py       # Neo4j / GraphML / GEXF 导出
│   └── assess_isolated_nodes.py  # 孤立节点快速评估
│
├── graph_viewer/              # 图谱可视化 + MCP 服务器
│   ├── main.go                # HTTP 服务（端口 10086）
│   ├── static/                # 前端静态资源（force-graph, three.js 等）
│   │   └── index.html         # 主页面（2D/3D 视图、箭头渲染、控件等）
│   └── mcp_server/            # MCP 服务器（stdio，供 Cursor 等接入）
│
├── OCR_raw/                  # 输入：OCR 文本（.p.txt + .txt 双源）
├── json_output_v4/           # 输出：实体 JSON + 全局索引 + 边 + 报告
├── exports/                  # 导出：Neo4j CSV / GraphML / GEXF
├── tests/                    # pytest 单元测试
├── gt_templates/             # Ground Truth 评估模板
│
├── requirements.txt          # Python 依赖
├── llm_config_example.json   # LLM 配置模板（不含密钥）
├── entity_aliases.json       # 实体别名表
├── example.graphy.json       # 图谱节点/边格式模板（任意 KG 可依此接入 viewer）
└── README.md                 # 完整文档
```

### 图谱可视化数据模板（example.graphy.json）

根目录的 `example.graphy.json` 定义了本系统对「节点 + 边」的数据约定，便于任意知识图谱接入：

- **graph_api**：`GET /api/graph` 的响应形状（节点需 `key` + `attributes.type/degree/file/description`，边需 `source`/`target` + `attributes.type` 或 `type`）。
- **file_format**：从目录加载时需 `global_entity_index.json`（entities 字典）+ `global_edges.json`（edges 数组）。
- **example**：最小可运行示例，可供其他项目复制后改写。

只要数据符合该模板（或由后端转换为该格式），即可用 `graph_viewer` 加载与展示。

## 核心数据流

```
OCR_raw/*.txt  ──→  pipeline.py (extract)  ──→  json_output_v4/*.json
                                                     │
                    pipeline.py (refine)   ←─────────┘
                         │
                         ▼
                  json_output_v4/global_*.json  ──→  scripts/enrich_*.py
                                                          │
                                                          ▼
                                                   exports/*  (Neo4j/GraphML)
```

## 关键约定

### entity_type 归一化

所有 entity_type 必须通过 `pipeline_lib.config.normalize_entity_type()` 归一化。
规则：
- `struct` / `structur` → `structure`
- `flag` → `flags`
- `enumvalue` → `enum_value`
- 长度 > 40 字符 → `unknown`
- 不在 `ALLOWED_ENTITY_TYPES` 白名单中 → `unknown`

**严禁**在 LLM 操作执行器、导出脚本中绕过归一化直接写入 entity_type。

### 边类型分层

| 层级 | 边类型 | 来源 |
|------|--------|------|
| 强语义 | `references`, `uses_type`, `parameter_type`, `return_type` | 提取/签名推断 |
| 结构层 | `belongs_to`, `member_of`, `contains` | LLM 精炼 |
| 层次归属 | `belongs_to_header`, `belongs_to_domain` | enrich_from_syntax |
| 弱语义 | `semantically_related` | embedding 兜底 |

评估孤立率时应区分"含层次边"和"纯语义边"两种口径。

### 文件命名

- `json_output_v4/_p_*.json` — Pass-1 提取结果（.p.txt 源）
- `json_output_v4/_t_*.json` — Pass-1 提取结果（.txt 源）
- `json_output_v4/<domain>.json` — 精炼后最终实体（无前缀）
- `json_output_v4/_*.json` — 报告、断点、日志
- `json_output_v4/global_*.json` — 全局索引和边

### 断点与恢复

- 提取断点：`_checkpoint.json`
- 精炼断点：`_llm_checkpoint.json`
- 操作日志：`_llm_operations.jsonl`（可回放）
- 恢复命令：`python pipeline.py --phase refine --resume`

## 开发规范

- **语言**: Python 3.10+
- **测试**: `python -m pytest tests/ -v`
- **类型归一化**: 任何写入 entity_type 的代码路径必须调用 `normalize_entity_type()`
- **配置**: 从 `pipeline_lib.config` 导入，不要在各脚本中维护副本
- **密钥**: `llm_config.json` 已在 `.gitignore`，只提交 `llm_config_example.json`
- **提交**: 不要提交 `json_output_v4/`（大文件）和 `exports/`

## 常用命令速查

```bash
# 完整流水线
python pipeline.py

# 仅提取 / 仅精炼
python pipeline.py --phase extract
python pipeline.py --phase refine --resume

# 增强脚本（在 scripts/ 下）
python scripts/enrich_from_syntax.py --apply
python scripts/kg_enrich_v43.py --strategy all --apply
python scripts/embed_link_isolated.py --apply

# 导出
python scripts/export_graph.py --format all

# 评估
python scripts/assess_isolated_nodes.py

# 图谱查看器
./start_graph_viewer.sh                    # 从项目根目录启动
cd graph_viewer && go run . --data ../json_output_v4  # 手动启动

# 测试
python -m pytest tests/ -v
```

## graph_viewer 功能说明

### 视图模式
- **2D 视图**：使用 Canvas 渲染，性能更好，适合大规模图谱
- **3D 视图**：使用 WebGL 渲染，支持空间导航，视觉效果更丰富
- 右上角可切换视图模式

### 边箭头
- **箭头显示**：2D 和 3D 视图均支持有向边箭头
- **箭头方向**：caller → callee（调用者指向被调用者）
- **箭头大小控制**：侧栏「Arrow size」滑块（20% - 150%），默认 67%
- **固定视觉大小**：箭头在视觉上保持固定大小，不随视图缩放变化
- **边类型过滤**：`semantically_related`、`via:*` 等弱语义边不显示箭头

### 侧栏控件
- **Min degree**：过滤低度数节点
- **Edge opacity**：边透明度（5% - 100%）
- **Node size**：节点大小（20% - 200%）
- **Edge width**：边宽度（5% - 100%）
- **Arrow size**：箭头大小（20% - 150%），默认 67%
- **Mem limit**：内存限制（128MB - 2048MB）

### 缓存与更新
- 服务器已配置 `Cache-Control: no-cache`，确保加载最新代码
- 开发时建议使用强制刷新（Ctrl+Shift+R / Cmd+Shift+R）
- 数据更新后需重启 graph_viewer 或使用 `--auto-reload` 选项
