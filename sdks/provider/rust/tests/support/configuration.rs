use serde_json::{Value, json};

pub fn configuration() -> Value {
    json!({
        "provider":{"name":"Independent Provider","description":"Provider declarations","public_origin":"HTTPS://Provider.Example:443/","recipient":"0x2222222222222222222222222222222222222222","services":[{"name":"OASF","endpoint":"ipfs://example","skills":["chat"],"score":0.1}]},
        "identity":{"provider_id":"42","wallet_address":"0x1111111111111111111111111111111111111111"},
        "chain":{"network":"testnet","max_age_ms":1000},
        "upstream":{"url":"http://127.0.0.1:8080/custom/v1/","api_key_env":"UPSTREAM_KEY"},
        "models":[{"model":"example/model","description":"Text generation","context_window":128000,"max_output":8192,"modalities":["text"],"embeddings":false,"streaming":true,"tools":true,"structured_output":true,"rates":{"input":"300000","output":"1200000"}}],
        "payment":{"collection_margin_ms":60000,"authorization_wait_ms":10000},
        "settlement":{"interval_ms":30000,"amount":"1000000"}
    })
}
