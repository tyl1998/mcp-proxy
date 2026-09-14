//! MRTR 出站标准头注入（MCP 协议修订 2026-07-28，SEP-2243）
//!
//! 后端（如 apitest `/mcp`，TS SDK 2.0.0）对携带 per-request envelope 的
//! 请求执行「标准头校验」：缺 `Mcp-Method` / `Mcp-Name`（tools/call 场景）
//! / `MCP-Protocol-Version`（须与 body envelope 交叉一致）之一即 400
//! `-32020`。rmcp 的 streamable http client transport 只有连接级
//! `custom_headers`，且 initialize 协商后自动注入的
//! `mcp-protocol-version` 是协商出的 legacy 版本——与 envelope 请求要求的
//! `2026-07-28` 冲突（header-body-version-mismatch）。
//!
//! 本模块包装 `StreamableHttpClient` trait，在 `post_message` 的最后出口处
//! 依据**实际序列化前的消息体**推导三个头（单一事实来源，交叉校验必然
//! 通过），仅对携带完整 envelope 的 `tools/call` 生效，其余请求原样透传。

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::BoxStream;
use http::{HeaderName, HeaderValue};
use rmcp::model::{ClientJsonRpcMessage, GetExtensions, Meta};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
};
use sse_stream::{Error as SseError, Sse};
use tracing::warn;

/// envelope 必填键（TS SDK `REQUIRED_ENVELOPE_KEYS`，
/// core 包 `auth-CUe6YdwF.mjs:24/47`）
pub const ENVELOPE_PROTOCOL_VERSION_KEY: &str = "io.modelcontextprotocol/protocolVersion";
pub const ENVELOPE_CLIENT_CAPABILITIES_KEY: &str = "io.modelcontextprotocol/clientCapabilities";

/// 从出站 `tools/call` 消息中提取 envelope 声明（协议版本 + 工具名）。
///
/// 仅当 params `_meta` 同时携带两个 envelope 键时视为 MRTR 请求
/// （与后端 `hasEnvelopeClaim` + `validateEnvelopeMeta` 的判定对齐：
/// 缺任一必填键会被后端 400，这里不注入头可让错误显式暴露而不是
/// 由代理伪造半套头）。
///
/// `_meta` 可能位于两处（`WithMeta::serialize` 在序列化时合并，extensions
/// 键优先）：
/// - `params.meta`：本地构造路径（proxy_handler 合并 context.meta 后设置）；
/// - `extensions`：反序列化路径（wire `_meta` 存进 extensions）以及
///   `send_request` 写入的 progress token 所在处。
fn extract_mrtr_envelope(message: &ClientJsonRpcMessage) -> Option<(String, String)> {
    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    let rmcp::model::ClientRequest::CallToolRequest(call_tool_request) = &request.request else {
        return None;
    };
    let params_meta = call_tool_request.params.meta.as_ref();
    let extensions_meta = call_tool_request.extensions().get::<Meta>();
    // 合并视图：extensions 键覆盖 params 键（与 WithMeta 序列化语义一致）
    let lookup = |key: &str| -> Option<&serde_json::Value> {
        extensions_meta
            .and_then(|m| m.0.get(key))
            .or_else(|| params_meta.and_then(|m| m.0.get(key)))
    };
    let version = lookup(ENVELOPE_PROTOCOL_VERSION_KEY)?;
    let capabilities = lookup(ENVELOPE_CLIENT_CAPABILITIES_KEY)?;
    if !version.is_string() || !capabilities.is_object() {
        return None;
    }
    let version = version.as_str()?.to_string();
    let name = call_tool_request.params.name.to_string();
    Some((version, name))
}

/// 依据 envelope 生成三个标准头；值非法（工具名含控制字符等罕见情形）
/// 时告警并跳过注入——请求照发，由后端显式 400 暴露问题。
fn mrtr_headers(version: &str, name: &str) -> Option<HashMap<HeaderName, HeaderValue>> {
    let protocol_version = match HeaderValue::from_str(version) {
        Ok(v) => v,
        Err(e) => {
            warn!("MRTR header skip: invalid protocol version {version:?}: {e}");
            return None;
        }
    };
    let mcp_name = match HeaderValue::from_str(name) {
        Ok(v) => v,
        Err(e) => {
            warn!("MRTR header skip: invalid tool name {name:?}: {e}");
            return None;
        }
    };
    let mut headers = HashMap::with_capacity(3);
    // 覆盖而非追加：transport 协商注入的 mcp-protocol-version 是 legacy
    // 版本，与 envelope 版本不一致会被后端 400（header-body-version-mismatch）
    headers.insert(
        HeaderName::from_static("mcp-protocol-version"),
        protocol_version,
    );
    headers.insert(
        HeaderName::from_static("mcp-method"),
        HeaderValue::from_static("tools/call"),
    );
    // Mcp-Name 明文即可（后端仅对 `base64:` sentinel 才解码）
    headers.insert(HeaderName::from_static("mcp-name"), mcp_name);
    Some(headers)
}

/// 注入 MRTR 标准头的出站客户端包装。
///
/// 泛型 `C` 通常是 `reqwest::Client`（rmcp 自带 `StreamableHttpClient` 实现），
/// 也可以继续叠加其他包装。
#[derive(Clone, Debug)]
pub struct MrtrHeaderClient<C> {
    inner: C,
}

impl<C> MrtrHeaderClient<C> {
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: StreamableHttpClient + Sync> StreamableHttpClient for MrtrHeaderClient<C> {
    type Error = C::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        mut custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        if let Some((version, name)) = extract_mrtr_envelope(&message) {
            if let Some(extra) = mrtr_headers(&version, &name) {
                custom_headers.extend(extra);
            }
        }
        self.inner
            .post_message(uri, message, session_id, auth_header, custom_headers)
            .await
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        self.inner
            .delete_session(uri, session_id, auth_header, custom_headers)
            .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        self.inner
            .get_stream(uri, session_id, last_event_id, auth_header, custom_headers)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CallToolRequestParams;
    use serde_json::json;

    fn message_from_json(value: serde_json::Value) -> ClientJsonRpcMessage {
        serde_json::from_value(value).expect("valid ClientJsonRpcMessage")
    }

    #[test]
    fn test_extract_envelope_from_tools_call() {
        let msg = message_from_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delete_environment",
                "arguments": { "environmentId": "env-1" },
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": { "elicitation": {} }
                },
                "inputResponses": {
                    "confirm": { "action": "accept", "content": { "confirm": true } }
                }
            }
        }));
        let (version, name) = extract_mrtr_envelope(&msg).expect("envelope should be detected");
        assert_eq!(version, "2026-07-28");
        assert_eq!(name, "delete_environment");
    }

    /// 本地构造路径（proxy_handler 合并 context.meta 后设置 params.meta，
    /// 不经过反序列化——此时 _meta 在 params.meta 而非 extensions）
    #[test]
    fn test_extract_envelope_from_locally_constructed_request() {
        let mut params = CallToolRequestParams::new("delete_environment");
        let mut meta = Meta::new();
        meta.0.insert(
            ENVELOPE_PROTOCOL_VERSION_KEY.to_string(),
            json!("2026-07-28"),
        );
        meta.0.insert(
            ENVELOPE_CLIENT_CAPABILITIES_KEY.to_string(),
            json!({ "elicitation": {} }),
        );
        params.meta = Some(meta);
        params.input_responses = Some(json!({
            "confirm": { "action": "decline" }
        }));
        let request = rmcp::model::ClientRequest::CallToolRequest(rmcp::model::Request::new(params));
        let msg = ClientJsonRpcMessage::Request(rmcp::model::JsonRpcRequest::new(
            rmcp::model::RequestId::Number(7),
            request,
        ));
        let (version, name) = extract_mrtr_envelope(&msg).expect("envelope should be detected");
        assert_eq!(version, "2026-07-28");
        assert_eq!(name, "delete_environment");
    }

    #[test]
    fn test_mrtr_headers_override_and_set() {
        let headers = mrtr_headers("2026-07-28", "delete_environment").unwrap();
        assert_eq!(
            headers.get(&HeaderName::from_static("mcp-protocol-version")).unwrap(),
            "2026-07-28"
        );
        assert_eq!(
            headers.get(&HeaderName::from_static("mcp-method")).unwrap(),
            "tools/call"
        );
        assert_eq!(
            headers.get(&HeaderName::from_static("mcp-name")).unwrap(),
            "delete_environment"
        );
    }

    #[test]
    fn test_no_envelope_without_meta() {
        let msg = message_from_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "list_things", "arguments": {} }
        }));
        assert!(extract_mrtr_envelope(&msg).is_none());
    }

    #[test]
    fn test_no_envelope_with_partial_meta() {
        // 只有 protocolVersion、缺 clientCapabilities → 不算完整 envelope，
        // 不注入头（后端会显式 400，暴露问题而不是被代理掩盖）
        let msg = message_from_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delete_environment",
                "_meta": { "io.modelcontextprotocol/protocolVersion": "2026-07-28" }
            }
        }));
        assert!(extract_mrtr_envelope(&msg).is_none());
    }

    #[test]
    fn test_no_envelope_on_other_methods() {
        let msg = message_from_json(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }));
        assert!(extract_mrtr_envelope(&msg).is_none());
    }

    /// vendor 补丁回归：params 顶层 `inputResponses` 必须在反序列化后
    /// 保留（未打补丁的 rmcp-soddygo 会静默丢弃该字段）。
    #[test]
    fn test_input_responses_survives_deserialization() {
        let msg = message_from_json(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "delete_environment",
                "arguments": { "environmentId": "env-1" },
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": { "elicitation": {} }
                },
                "inputResponses": {
                    "confirm": { "action": "accept", "content": { "confirm": true } }
                }
            }
        }));
        let ClientJsonRpcMessage::Request(request) = &msg else {
            panic!("expected request");
        };
        let rmcp::model::ClientRequest::CallToolRequest(call_tool_request) = &request.request
        else {
            panic!("expected tools/call");
        };
        let params: &CallToolRequestParams = &call_tool_request.params;
        let responses = params
            .input_responses
            .as_ref()
            .expect("inputResponses must survive deserialization (vendored patch)");
        assert_eq!(
            responses["confirm"]["action"],
            json!("accept"),
            "response entry field name on the wire is `action`, not `kind`"
        );
        // 序列化 roundtrip：顶层位置不漂移
        let re = serde_json::to_value(&msg).unwrap();
        assert!(re["params"].as_object().unwrap().contains_key("inputResponses"));
        assert!(re["params"]["inputResponses"]["confirm"]["action"] == json!("accept"));
    }

    /// vendor 补丁回归（结果侧）：2026 wire codec 把 `resultType`/`inputRequests`
    /// 放在 result **顶层**（不在 structuredContent 里）。未打补丁的 rmcp-soddygo
    /// 靠 `_meta` 命中 CallToolResult 变体后把这两个字段静默丢弃——确认表单被吞，
    /// 客户端只看到 `content: []`（Phase B 联调实测的「模型反复重试」根因）。
    #[test]
    fn test_call_tool_result_flat_mrtr_fields_survive() {
        use rmcp::model::{CallToolResult, ServerResult};
        // apitest 真实响应形状（TS SDK 2.0.0 实测）
        let wire = json!({
            "resultType": "input_required",
            "inputRequests": {
                "confirm": {
                    "method": "elicitation/create",
                    "params": {
                        "message": "Delete environment \"staging\"?",
                        "requestedSchema": {
                            "type": "object",
                            "properties": { "confirm": { "type": "boolean" } },
                            "required": ["confirm"]
                        },
                        "mode": "form"
                    }
                }
            },
            "_meta": {
                "io.modelcontextprotocol/serverInfo": { "name": "apitest", "version": "0.1.0" }
            }
        });
        let result: CallToolResult = serde_json::from_value(wire.clone())
            .expect("flat MRTR result must deserialize (vendored patch)");
        assert_eq!(result.result_type.as_deref(), Some("input_required"));
        let requests = result.input_requests.as_ref().expect("inputRequests kept");
        assert_eq!(requests["confirm"]["method"], json!("elicitation/create"));
        assert!(requests["confirm"]["params"]["message"].is_string());
        // 再序列化：顶层位置与字段名不漂移（代理透传不丢形状）
        let re = serde_json::to_value(&result).unwrap();
        assert_eq!(re["resultType"], json!("input_required"));
        assert!(re["inputRequests"]["confirm"]["params"]["requestedSchema"].is_object());
        assert!(!re.as_object().unwrap().contains_key("structuredContent"));

        // ServerResult untagged 枚举也能命中 CallToolResult 变体（proxy 客户端路径）
        let server_result: ServerResult =
            serde_json::from_value(wire).expect("ServerResult enum must match CallToolResult");
        assert!(matches!(server_result, ServerResult::CallToolResult(_)));

        // complete 形状（content + structuredContent + 顶层 resultType）同样无损
        let complete: CallToolResult = serde_json::from_value(json!({
            "content": [{ "type": "text", "text": "{\"deleted\":true}" }],
            "structuredContent": { "deleted": true, "affectedEndpoints": 0 },
            "resultType": "complete",
            "_meta": { "io.modelcontextprotocol/serverInfo": { "name": "apitest", "version": "0.1.0" } }
        }))
        .expect("complete result with flat resultType");
        assert_eq!(complete.result_type.as_deref(), Some("complete"));
        assert_eq!(complete.structured_content.as_ref().unwrap()["deleted"], json!(true));
    }
}
