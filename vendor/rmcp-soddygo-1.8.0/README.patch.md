# rmcp-soddygo 1.8.0 本地补丁说明

上游：https://github.com/modelcontextprotocol/rust-sdk 的社区 fork
`rmcp-soddygo`（crates.io 发布版源码，校验和与 Cargo.lock 中
`971e57ac…44fd3` 一致，下载自 static.crates.io）

## 为什么 vendor

mcp-streamable-proxy 依赖 rmcp-soddygo 1.8.0。官方 1.8.0 的
`CallToolRequestParams` 只有 `meta`/`name`/`arguments`/`task` 四个字段，
缺少 MCP 协议修订 **2026-07-28**（MRTR，Multi-Round Tool Result）的
params **顶层**保留字 `inputResponses`（TS SDK `RETRY_PARAMS_KEYS`，
`src-CX2iR2pK.mjs:5992`）。serde 默认忽略未知字段，导致：

- 入站（Java → proxy）：`tools/call` 第二回合的 `inputResponses` 在反序列化
  时被静默丢弃；
- 出站（proxy → 后端）：typed `peer.call_tool` 无法序列化出顶层
  `inputResponses`。

两处叠加使确认回合永远退化为新的第一回合（Phase A 联调失败的根因之一，
另见总方案 §2.5「`inputResponses` 放 `_meta` 仍 input_required」）。

## 补丁内容（相对 crates.io 1.8.0）

1. `src/model.rs` `CallToolRequestParams`：
   - 新增 `#[serde(rename = "inputResponses", default, skip_serializing_if = "Option::is_none")] pub input_responses: Option<serde_json::Value>`
   - `new()` 构造补字段；新增 `with_input_responses()` 链式构造
2. `src/handler/server/tool.rs` `ToolCallContext`：新增 `input_responses`
   字段（从 `CallToolRequestParams` 解构传入），供 server 侧 handler 访问
3. 测试代码（`src/model/serde_impl.rs` ×6、`src/handler/server/router/tool.rs`
   ×2 的 `#[cfg(test)]` 字面量）补 `input_responses: None`

注意：`tests/test_message_schema/` 的 JSON schema 快照未更新（workspace
构建未启用 `schemars` feature，快照测试不在构建路径上；若需在 vendored
crate 内跑 `cargo test --features schemars`，先 `UPDATE_SCHEMA=1` 重新生成）。

## 维护

- 升级 rmcp-soddygo 前先检查新版本 `CallToolRequestParams` 是否已原生带
  `inputResponses`（官方 rust-sdk 跟进 2026-07-28 修订后），能升则删除本
  目录与根 Cargo.toml `[patch.crates-io]` 的对应条目
- `rmcp-soddygo-macros` 等 proc-macro 依赖仍走 crates.io，无需 vendor
