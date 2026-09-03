# tools/ — AGENTS.md

## 组件事实

| 字段 | 值 |
|------|-----|
| schema_version | 1 |
| component_id | tools |
| internal_version | 0.1.0 |
| updated | 2026-09-02 |
| owner | P2,P5 |
| role | 本仓文档治理检查器，不进入运行时插件发现 |
| core_files | tools/AGENTS.md, tools/agents_doccheck/check.py |
| public_entry | tools/agents_doccheck/check.py |
| upstream | - |
| downstream | winapi_graph |
| config_entry | - |
| outputs | - |

## 公开接口

| 接口 | 类型 | 稳定性 | owner 路径 |
|------|------|--------|------------|
| agents_doccheck | cli | public | tools/agents_doccheck/check.py |

> 所属方向: P2,P5 — 开发工具

## 工具清单

| 目录 | 用途 | 启动方式 | 备注 |
|------|------|----------|------|
| `agents_doccheck/` | 本仓文档依赖传播检查器：校验受管 `AGENTS.md` 事实表、三列组合矩阵、失效引用与触发目录流水账 | `python tools/agents_doccheck/check.py` | 失败非零退出；无跳过开关；不检查 Hub gitlink |
