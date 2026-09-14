# rmcp 0.10.0 本地补丁说明

上游：https://github.com/modelcontextprotocol/rust-sdk （rmcp 0.10.0，crates.io 发布版源码）

## 为什么 vendor

mcp-sse-proxy 依赖 rmcp 0.10（SSE server transport 在 0.12 被移除，无法升级）。
官方 0.10 的 `CallToolRequestParam` 缺少 `_meta` 字段，导致经 SSE 代理转发
`tools/call` 时请求 envelope 里的 `_meta`（MCP 2025-06-18+ MRTR 协议的
`inputResponses` 载体）在反序列化时被静默丢弃——MRTR 第二回合（用户确认后
重发）永远到不了后端。官方 0.11+ 已原生带该字段，但 SSE server 支持未回移。

## 补丁内容（相对 crates.io 0.10.0）

1. `src/model.rs` `CallToolRequestParam`：
   - 新增 `#[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")] pub meta: Option<Meta>`
   - 新增 `new` / `with_arguments` / `with_meta` 构造方法
2. `src/handler/server/tool.rs` `ToolCallContext`：携带 `meta` 字段（从
   `CallToolRequestParam` 解构传入），供 server 侧 handler 访问
3. `src/model.rs` `ProtocolVersion`：新增 `V_2026_07_28` 常量 + 反序列化
   分支（MRTR 协议版本，sse 侧透传后端协商结果时需要能表示该版本串）

## 维护

- `Meta` 类型复用本版本已有的 `model::Meta`（JsonObject 透明包装），无新依赖
- 升级 rmcp 前先检查官方版本是否已含 `_meta` 且支持 SSE server，能升则删除本目录与根 Cargo.toml 的 `[patch.crates-io]` 段
