use rmcp::model::{CallToolRequestParam, Meta};
use serde_json::{Value, json};

/// 直接对 CallToolRequestParam 做 wire 级 roundtrip（模拟 sse 侧收到的 params）
fn roundtrip_param(meta: Option<Value>) -> Value {
    let mut params = json!({"name": "delete_environment", "arguments": {"environmentId": "e1"}});
    if let Some(m) = meta {
        params["_meta"] = m;
    }
    let p: CallToolRequestParam = serde_json::from_value(params).expect("deserialize param");
    serde_json::to_value(&p).expect("serialize param")
}

#[test]
fn param_meta_roundtrip() {
    let v = roundtrip_param(Some(json!({
        "inputResponses": {"confirm": {"kind": "accept", "content": {"confirm": true}}}
    })));
    println!("param: {v}");
    let meta = v.get("_meta").expect("_meta must survive roundtrip");
    assert_eq!(meta["inputResponses"]["confirm"]["kind"], "accept");
    assert_eq!(meta["inputResponses"]["confirm"]["content"]["confirm"], true);
}

#[test]
fn param_without_meta_stays_clean() {
    let v = roundtrip_param(None);
    println!("param: {v}");
    assert!(v.get("_meta").is_none(), "no _meta when absent: {v}");
    assert_eq!(v["name"], "delete_environment");
    assert_eq!(v["arguments"]["environmentId"], "e1");
}

/// ContextMeta 模拟：serve_loop 把 extensions 里的 meta swap 进 context —— 在 sse 侧
/// BackendSessionHandler.call_tool 里我们拿到的 CallToolRequestParam.meta 是否还带着
#[test]
fn meta_construction_helpers() {
    let mut p = CallToolRequestParam::new("delete_environment")
        .with_arguments(json!({"environmentId": "e1"}).as_object().unwrap().clone());
    assert!(p.meta.is_none());
    let mut m = Meta::new();
    // Meta(pub JsonObject) - 手动构建 inputResponses
    let responses = json!({"confirm": {"kind": "decline"}});
    m.0.insert("inputResponses".to_string(), responses);
    p = p.with_meta(m);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["_meta"]["inputResponses"]["confirm"]["kind"], "decline");
}
