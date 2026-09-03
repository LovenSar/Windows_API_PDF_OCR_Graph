# Windows_API_PDF_OCR_Graph 开发规范

> 最后更新：2026-09-02

本仓是 SecOrchestrate 卫星仓。**本篇是本仓文档依赖传播协议的唯一 owner。** 根 `AGENTS.md` 只放约束入口并链接本节，不复制协议正文。

全局约束摘要（本篇自足）：

- **English-Only UI**：所有用户可见文案必须为英文。
- **隐私门禁**：行为 RED(≥80) 默认本地；仅管理员信任的 mTLS Worker 可收研究行为原文；凭据/私钥/令牌等不可降级发现始终本机；可信云端路径必须脱敏并复检。

## 1. 文档依赖传播协议

四条核心不变量：

1. **事实只有一个 owner。** 完整定义只在最接近实现的一层维护；上层只引用。
2. **修改从叶子向根做影响检查，不机械传播。** 父层可见事实未变则不改父层正文、不升父版本；检查必须做完。
3. **AGENTS 保存当前状态，CHANGELOG 保存历史。** 禁止在 `AGENTS.md` 追加流水账。
4. **版本描述组件事实，README 描述组件组合。** 产品版本与组件内部版本是两个维度，不强制同步。

违反文档依赖传播协议不产生“警告通过”状态。凡影响当前组件事实、接口、版本或上层可见事实的违规，均视为任务未完成，必须在当前任务内修复并重新通过 `agents_doccheck`。

职责分离：

| 文件 | 只回答 | 禁止 |
|------|--------|------|
| 各层 `AGENTS.md` | 现在这个目录应该怎样开发 | 流水账、发布历史、复制下层完整签名 |
| 根 `README.md` | 项目现在是什么（定位、启动、组合版本） | 实现细节、变更年表 |
| 根 `CHANGELOG.md` | 以前发生过什么 | 当前开发规范 |

P1–P5 的职责定义由 LLM_AAV Hub 仓库的 docs/WORK_DIVISION.md 唯一维护。`AGENTS.md` 事实表不解释方向含义。

### 1.1 开发门禁

1. **先读作用域链**：从根 `AGENTS.md` 读到目标目录 `AGENTS.md`；越靠近文件优先级越高。
2. **先定唯一所属组件**（用 `component_id`，不要只靠路径），再改代码。
3. **门禁顺序**：代码 → 测试 → 接口检查 → 当前层文档融合 → 父层影响检查 → 根 README 组合版本检查。任一步未完成不算任务结束。
4. **叶子 → 根**：仅当父层能力、入口、依赖、公开接口或组件版本变化时改父层正文。
5. **没有影响就不改上层正文，但必须完成影响检查**（写在评审说明，不在父 `AGENTS.md` 盖章）。
6. **增 / 改 / 删 / 重命名**都要清上层失效引用。
7. 勿在 `docs/` 根放过程文档（日报、BUG 清单、一次性交接）；历史见 Git 与根 `CHANGELOG.md`。

跨仓组合边界由 Hub 核对卫星根版本与 gitlink pin。本仓检查器不把叶子版本传播到 Hub。

### 1.2 接口

凡被其他模块依赖即接口：HTTP、Go/Python 导出、CLI、环境变量、配置、JSON/YAML schema、文件 I/O、数据库结构、event schema。

稳定性标记：`public` / `internal` / `deprecated`。完整签名只在 owner 层维护；上层只写稳定入口并链接 owner。

### 1.3 版本

每层内部版本使用 `MAJOR.MINOR.PATCH`。

- 破坏已有接口升 `MAJOR`
- 增加向后兼容能力升 `MINOR`
- 文档修改反映组件事实、接口、约束或行为变化时升 `PATCH`
- 仅修复拼写、格式、链接和措辞，不升组件版本，也不改 `updated`

父版本不机械跟随子 `PATCH`。子组件变化若未改变父层可见能力、入口、依赖、公开接口或组件版本，父层不改正文、不升版本。

`0.1.0` 表示该组件首次纳入统一版本治理，不表示该组件第一次实现或第一次发布。

产品发布版本写在根 `README.md`，例如 `Windows_API_PDF_OCR_Graph v0.1.0 (2026-09-02)`。根组件 `component_id: winapi_graph` 的 `internal_version` 是根目录治理版本，**不进入** README 组合矩阵。两者允许独立变化。

叶子有自己的内部版本，但不因此进入 README 矩阵。

### 1.4 组件事实表（schema_version: 1）

每个受管 `AGENTS.md` 顶部维护 `## 组件事实` 两列表。字段名与语法锁死；事实表结构变化才升 `schema_version`，不升 `internal_version`。

| 字段 | 规则 |
|------|------|
| `schema_version` | 整数，起步 `1` |
| `component_id` | 稳定标识，目录重命名后不变，仓库内唯一 |
| `internal_version` | `MAJOR.MINOR.PATCH` |
| `updated` | `YYYY-MM-DD`。仅当组件事实发生有效变化时更新 |
| `owner` | `P3` 或 `P2,P4`。禁止 `P1–P5` 或「所有人」 |
| `role` | 一句话职责 |
| `core_files` | 逗号+空格分隔的仓库相对路径，一律 `/`，禁止 `\`，禁止子列表 |
| `public_entry` | 对外入口 |
| `upstream` | 上游 `component_id`，逗号+空格；无则 `-` |
| `downstream` | 下游 `component_id`，逗号+空格；无则 `-` |
| `config_entry` | 环境变量 / 配置 / schema；无则 `-` |
| `outputs` | 产物；无则 `-` |

公开接口另用 `## 公开接口` 短表：`接口 | 类型 | 稳定性 | owner 路径`。`owner 路径` 必须是仓库相对路径且真实存在。

功能变化后把新事实融合进职责、文件、接口、约束；删除过期描述。需要历史时写入根 `CHANGELOG.md`，不在 `AGENTS.md` 追加修改记录。下次修改已有追加债务的目录时再融合，不借治理基线重写全部正文。

### 1.5 受管目录

受管集合 = 已存在 `AGENTS.md` 的目录 ∪ 下列显式登记的治理目录：

- `docs/`
- `tools/`

仓库根因已有 `AGENTS.md` 而受管，`component_id` 为 `winapi_graph`。不要隐含「所有源码目录必须有 AGENTS」。缓存、构建产物、测试 fixture 不在受管集合内。新增独立受管组件时才新建 `AGENTS.md`。`json_output_v4/` 与 `exports/` 不是受管组件。

README 组合矩阵只列这 2 个 `component_id`：`docs`、`tools`。矩阵列为 `component_id + path + internal_version`。不要 pin 列。

一致性检查：`python tools/agents_doccheck/check.py`。检查器只验证本节已规定的制度，不创造新规则。失败即非零退出，表示任务未完成；修复后必须重新跑完整检查。检查器不提供跳过项、目录白名单或缩小检查范围的成功路径。
