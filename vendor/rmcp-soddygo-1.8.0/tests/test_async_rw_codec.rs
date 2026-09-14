use futures::{SinkExt, StreamExt};
use rmcp::transport::async_rw::{JsonRpcMessageCodec, JsonRpcMessageCodecError};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Encoder, FramedRead, FramedWrite},
};

// ----- helpers -----

fn from_async_read<T: DeserializeOwned, R: AsyncRead>(reader: R) -> impl futures::Stream<Item = T> {
    FramedRead::new(reader, JsonRpcMessageCodec::<T>::default()).filter_map(|result| {
        if let Err(e) = &result {
            tracing::error!("Error reading from stream: {}", e);
        }
        futures::future::ready(result.ok())
    })
}

fn from_async_write<T: Serialize, W: AsyncWrite + Send>(
    writer: W,
) -> impl futures::Sink<T, Error = std::io::Error> {
    FramedWrite::new(writer, JsonRpcMessageCodec::<T>::default()).sink_map_err(Into::into)
}

/// Helper: feed a single line (appended with '\n') into the codec and return the decode result.
fn decode_single_line(line: &str) -> Result<Option<serde_json::Value>, JsonRpcMessageCodecError> {
    let mut codec = JsonRpcMessageCodec::<serde_json::Value>::default();
    let mut buf = BytesMut::from(format!("{line}\n").as_str());
    codec.decode(&mut buf)
}

// ===== Encode / Decode round-trip =====

#[tokio::test]
async fn test_decode() {
    let data = r#"{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":1}
    {"jsonrpc":"2.0","method":"subtract","params":[23,42],"id":2}
    {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":3}
    {"jsonrpc":"2.0","method":"subtract","params":[23,42],"id":4}
    {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":5}
    {"jsonrpc":"2.0","method":"subtract","params":[23,42],"id":6}
    {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":7}
    {"jsonrpc":"2.0","method":"subtract","params":[23,42],"id":8}
    {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":9}
    {"jsonrpc":"2.0","method":"subtract","params":[23,42],"id":10}

    "#;

    let mut cursor = BufReader::new(data.as_bytes());
    let mut stream = from_async_read::<serde_json::Value, _>(&mut cursor);

    for i in 1..=10 {
        let item = stream.next().await.unwrap();
        assert_eq!(
            item,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": if i % 2 != 0 { [42, 23] } else { [23, 42] },
                "id": i,
            })
        );
    }
}

#[tokio::test]
async fn test_encode() {
    let test_messages = vec![
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subtract",
            "params": [42, 23],
            "id": 1,
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subtract",
            "params": [23, 42],
            "id": 2,
        }),
    ];

    let mut buffer = Vec::new();
    let mut writer = from_async_write(&mut buffer);

    for message in test_messages.iter() {
        writer.send(message.clone()).await.unwrap();
    }
    writer.close().await.unwrap();
    drop(writer);

    let output = String::from_utf8_lossy(&buffer);
    let mut lines = output.lines();

    for expected_message in test_messages {
        let line = lines.next().unwrap();
        let parsed_message: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(parsed_message, expected_message);
    }

    assert!(lines.next().is_none());
}

// ===== Standard notification filtering =====

#[test]
fn test_standard_notification_preserved() {
    // Standard MCP notifications should be decoded successfully
    let standard =
        r#"{"method":"notifications/message","params":{"level":"info","data":"standard"}}"#;
    let progress =
        r#"{"method":"notifications/progress","params":{"progressToken":"token","progress":50}}"#;

    let result_standard = decode_single_line(standard);
    assert!(result_standard.is_ok());
    assert!(
        result_standard.unwrap().is_some(),
        "Standard notification should be preserved"
    );

    let result_progress = decode_single_line(progress);
    assert!(result_progress.is_ok());
    assert!(
        result_progress.unwrap().is_some(),
        "Progress notification should be preserved"
    );
}

#[test]
fn test_non_standard_notification_handled_gracefully() {
    // Non-standard notifications are valid JSON, so when T = serde_json::Value they
    // parse successfully. The compatibility filtering only applies when T is a typed
    // MCP message where the initial parse fails.
    let stderr_msg = r#"{"method":"notifications/stderr","params":{"content":"stderr message"}}"#;
    let custom_msg = r#"{"method":"notifications/custom","params":{"data":"custom"}}"#;

    let result_stderr = decode_single_line(stderr_msg);
    assert!(
        result_stderr.is_ok(),
        "Non-standard notification should not error"
    );

    let result_custom = decode_single_line(custom_msg);
    assert!(
        result_custom.is_ok(),
        "Non-standard notification should not error"
    );
}

#[test]
fn test_all_standard_notifications_recognised() {
    let standard_notifications = [
        "notifications/cancelled",
        "notifications/initialized",
        "notifications/message",
        "notifications/progress",
        "notifications/prompts/list_changed",
        "notifications/resources/list_changed",
        "notifications/resources/updated",
        "notifications/roots/list_changed",
        "notifications/tools/list_changed",
    ];

    for method in standard_notifications {
        let msg = format!(r#"{{"method":"{method}","params":{{}}}}"#);
        let result = decode_single_line(&msg);
        assert!(
            result.is_ok(),
            "Standard notification '{method}' should not error"
        );
        assert!(
            result.unwrap().is_some(),
            "Standard notification '{method}' should be preserved (not skipped)"
        );
    }
}

#[test]
fn test_non_mcp_notification_handled_gracefully() {
    // Non-MCP method notifications are valid JSON, so when T = serde_json::Value they
    // parse successfully. They should at minimum not cause errors.
    let non_mcp = r#"{"method":"some/other/method","params":{}}"#;
    let result = decode_single_line(non_mcp);
    assert!(result.is_ok(), "Non-MCP notification should not error");
}

// ===== Non-JSON content tolerance =====

#[test]
fn test_non_json_content_skipped() {
    let cases = [
        ("plain text log", "This is a plain text log message"),
        (
            "log with timestamp",
            "[2024-01-15 10:30:45] INFO: Server started on port 8080",
        ),
        ("empty line", ""),
        ("whitespace only", "   "),
    ];

    for (label, content) in cases {
        let result = decode_single_line(content);
        assert!(result.is_ok(), "{label} should not cause error");
        assert!(result.unwrap().is_none(), "{label} should be skipped");
    }
}

// ===== Streaming: mixed JSON and non-JSON =====

#[tokio::test]
async fn test_decode_with_non_json_lines() {
    let data = r#"This is a log message that should be skipped
{"jsonrpc":"2.0","method":"test","params":{},"id":1}
[INFO] Another log line
{"jsonrpc":"2.0","method":"test","params":{},"id":2}
Final log message

"#;

    let mut cursor = BufReader::new(data.as_bytes());
    let mut stream = from_async_read::<serde_json::Value, _>(&mut cursor);

    let item1 = stream.next().await.unwrap();
    assert_eq!(item1["id"], 1);

    let item2 = stream.next().await.unwrap();
    assert_eq!(item2["id"], 2);

    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn test_framed_read_behavior_with_skipped_lines() {
    // FramedRead should continue reading when decode returns Ok(None) for non-JSON content
    let data = b"Log line 1\n{\"id\":1}\nLog line 2\n{\"id\":2}\nLog line 3\n";

    let reader = BufReader::new(&data[..]);
    let mut stream = FramedRead::new(reader, JsonRpcMessageCodec::<serde_json::Value>::default());

    let msg1 = stream.next().await;
    assert!(msg1.is_some(), "Should receive first message");
    let msg1 = msg1.unwrap();
    assert!(msg1.is_ok(), "First message should be valid");
    assert_eq!(msg1.unwrap()["id"], 1);

    let msg2 = stream.next().await;
    assert!(msg2.is_some(), "Should receive second message");
    let msg2 = msg2.unwrap();
    assert!(msg2.is_ok(), "Second message should be valid");
    assert_eq!(msg2.unwrap()["id"], 2);

    let msg3 = stream.next().await;
    assert!(msg3.is_none(), "Stream should end after all valid messages");
}

#[tokio::test]
async fn test_initialization_with_log_noise() {
    // Simulate server output: log lines, then initialize response, more logs, then another response
    let data = b"[INFO] Server starting...\n[DEBUG] Loading config...\n{\"jsonrpc\":\"2.0\",\"result\":{\"protocolVersion\":\"2024-11-05\"},\"id\":1}\n[INFO] Ready\n{\"jsonrpc\":\"2.0\",\"result\":{},\"id\":2}\n";

    let reader = BufReader::new(&data[..]);
    let mut stream = FramedRead::new(reader, JsonRpcMessageCodec::<serde_json::Value>::default());

    let resp1 = stream.next().await.unwrap().unwrap();
    assert_eq!(resp1["id"], 1);
    assert!(resp1.get("result").is_some());

    let resp2 = stream.next().await.unwrap().unwrap();
    assert_eq!(resp2["id"], 2);

    assert!(stream.next().await.is_none());
}

// ===== Typed message notification filtering =====
// When T is a concrete MCP message type (not serde_json::Value),
// non-standard notifications fail the initial parse and trigger the
// compatibility filtering path, which skips them.

#[test]
fn test_non_standard_notification_filtered_with_typed_message() {
    let mut codec = JsonRpcMessageCodec::<rmcp::model::ServerJsonRpcMessage>::default();

    // Non-standard notification — should be silently skipped (Ok(None))
    let stderr_notif = r#"{"method":"notifications/stderr","params":{"content":"some log"}}"#;
    let mut buf = BytesMut::from(format!("{stderr_notif}\n").as_str());
    let result = codec.decode(&mut buf);
    assert!(result.is_ok(), "Non-standard notification should not error");
    assert!(
        result.unwrap().is_none(),
        "Non-standard notification should be filtered out for typed message"
    );
}

#[test]
fn test_non_mcp_notification_filtered_with_typed_message() {
    let mut codec = JsonRpcMessageCodec::<rmcp::model::ServerJsonRpcMessage>::default();

    // Non-MCP method (e.g. LSP) without id — should be silently skipped
    let non_mcp = r#"{"method":"textDocument/didOpen","params":{}}"#;
    let mut buf = BytesMut::from(format!("{non_mcp}\n").as_str());
    let result = codec.decode(&mut buf);
    assert!(result.is_ok(), "Non-MCP notification should not error");
    assert!(
        result.unwrap().is_none(),
        "Non-MCP notification should be filtered out for typed message"
    );
}

#[test]
fn test_standard_notification_decoded_with_typed_message() {
    let mut codec = JsonRpcMessageCodec::<rmcp::model::ServerJsonRpcMessage>::default();

    // Standard MCP notification — should parse successfully
    let cancelled = r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"reason":"timeout"}}"#;
    let mut buf = BytesMut::from(format!("{cancelled}\n").as_str());
    let result = codec.decode(&mut buf);
    assert!(result.is_ok(), "Standard notification should not error");
    assert!(
        result.unwrap().is_some(),
        "Standard notification should be decoded for typed message"
    );
}

#[test]
fn test_codec_encode_decode_roundtrip() {
    let msg = serde_json::json!({"jsonrpc":"2.0","method":"ping","id":42});
    let mut codec = JsonRpcMessageCodec::<serde_json::Value>::default();
    let mut buf = BytesMut::new();

    codec.encode(msg.clone(), &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();

    assert_eq!(decoded, msg);
}

#[test]
fn test_codec_decode_empty_buffer_returns_none() {
    let mut codec = JsonRpcMessageCodec::<serde_json::Value>::default();
    let mut buf = BytesMut::new();

    let result = codec.decode(&mut buf).unwrap();
    assert!(result.is_none(), "Empty buffer should return None");
}

#[test]
fn test_codec_max_length() {
    let mut codec = JsonRpcMessageCodec::<serde_json::Value>::new_with_max_length(10);
    assert_eq!(codec.max_length(), 10);

    // Line longer than max_length should return MaxLineLengthExceeded
    let mut buf = BytesMut::from("{\"very_long_key\":\"very_long_value\"}\n");
    let result = codec.decode(&mut buf);

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        JsonRpcMessageCodecError::MaxLineLengthExceeded
    ));
}
