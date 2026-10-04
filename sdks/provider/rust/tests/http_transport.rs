use alloy_primitives::{U256, hex};
use mppi_provider::{
    chain::Rpc,
    http::{HttpJsonRpc, HttpTransport},
};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

fn server(
    response: impl FnOnce(Vec<u8>) -> Vec<u8> + Send + 'static,
) -> (String, mpsc::Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://{}/rpc/key?network=arc",
        listener.local_addr().unwrap()
    );
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let read = socket.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let length = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let _ = tx.send(request.clone());
        let _ = socket.write_all(&response(request));
    });
    (url, rx, worker)
}
fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    [
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
            body.len()
        )
        .into_bytes(),
        body.to_vec(),
    ]
    .concat()
}

#[tokio::test]
async fn full_catalog_survives_abi_hex_and_rpc_envelope() {
    const LIMIT: usize = 1 << 20;
    let base = r#"[{"input":"1","model":"/b","output":"2"}]"#;
    let catalog = base
        .replace("/b", &format!("{}/b", "n".repeat(LIMIT - base.len())))
        .into_bytes();
    assert_eq!(catalog.len(), LIMIT);
    mppi_provider::parse_catalog(&catalog, LIMIT).unwrap();
    let transport = HttpTransport::new(2000, 4 << 20, None).unwrap();
    let expected = catalog.clone();
    let (url, _requests, worker) = server(move |request| {
        let offset = request.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
        let request: Value = serde_json::from_slice(&request[offset..]).unwrap();
        let mut abi = U256::from(32u64).to_be_bytes::<32>().to_vec();
        abi.extend_from_slice(&U256::from(catalog.len()).to_be_bytes::<32>());
        abi.extend_from_slice(&catalog);
        let body = serde_json::to_vec(
            &json!({"jsonrpc":"2.0","id":request["id"],"result":format!("0x{}",hex::encode(abi))}),
        )
        .unwrap();
        assert!(body.len() > 2 * LIMIT);
        response("200 OK", "Content-Type: application/json\r\n", &body)
    });
    let result = HttpJsonRpc::new(url, transport)
        .unwrap()
        .call("eth_call", json!([]))
        .await;
    worker.join().unwrap();
    let result = result.unwrap();
    let raw = hex::decode(result.as_str().unwrap().strip_prefix("0x").unwrap()).unwrap();
    assert_eq!(U256::from_be_slice(&raw[..32]), U256::from(32u64));
    assert_eq!(U256::from_be_slice(&raw[32..64]), U256::from(LIMIT));
    let parsed = mppi_provider::parse_catalog(&raw[64..], LIMIT).unwrap();
    assert_eq!(parsed.canonical_bytes(), expected);
    assert!(HttpTransport::new(1000, (4 << 20) + 1, None).is_err());
    assert!(mppi_provider::discovery::parse_document(&vec![b' '; LIMIT + 1], LIMIT).is_err());
}
#[tokio::test]
async fn strict_http_status_type_encoding_and_streamed_size_limits() {
    for (status, headers, body, limit, ok) in [
        (
            "200 OK",
            "Content-Type: application/json\r\n",
            b"{\"ok\":true}".as_slice(),
            20,
            true,
        ),
        (
            "302 Found",
            "Content-Type: application/json\r\nLocation: /secret\r\n",
            b"{}".as_slice(),
            20,
            false,
        ),
        (
            "200 OK",
            "Content-Type: text/html\r\n",
            b"{}".as_slice(),
            20,
            false,
        ),
        (
            "200 OK",
            "Content-Type: application/json\r\nContent-Encoding: gzip\r\n",
            b"{}".as_slice(),
            20,
            false,
        ),
        (
            "200 OK",
            "Content-Type: application/json\r\n",
            b"123456789".as_slice(),
            8,
            false,
        ),
        (
            "200 OK",
            "Content-Type: application/json\r\nContent-Type: application/json\r\n",
            b"{}".as_slice(),
            20,
            false,
        ),
    ] {
        let (url, requests, worker) = server(move |_| response(status, headers, body));
        let result = HttpTransport::new(1000, limit, None)
            .unwrap()
            .request(&url, None)
            .await;
        assert_eq!(result.is_ok(), ok);
        assert!(
            String::from_utf8(requests.recv().unwrap())
                .unwrap()
                .starts_with("GET /rpc/key?network=arc HTTP/1.1\r\n")
        );
        worker.join().unwrap();
    }
    // No Content-Length: response is bounded while reading chunks too.
    let (url, _, worker) = server(|_| {
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n123456789"
            .to_vec()
    });
    assert!(
        HttpTransport::new(1000, 8, None)
            .unwrap()
            .request(&url, None)
            .await
            .is_err()
    );
    worker.join().unwrap();
}
#[tokio::test]
async fn rpc_correlates_and_rejects_duplicate_remote_fields_without_secrets() {
    for mode in 0..5 {
        let (url, _requests, worker) = server(move |request| {
            let offset = request.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
            let request: Value = serde_json::from_slice(&request[offset..]).unwrap();
            assert_eq!(request["method"], "eth_chainId");
            assert_eq!(request["params"], json!([]));
            let id = serde_json::to_string(&request["id"]).unwrap();
            let body = match mode {
                0 => format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":\"0x4cef52\"}}"),
                1 => "{\"jsonrpc\":\"2.0\",\"id\":\"wrong\",\"result\":1}".into(),
                2 => format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":1,\"error\":\"secret-token\"}}"
                ),
                3 => format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":1,\"result\":2}}"),
                _ => format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"x\":1,\"\\u0078\":2}}}}"
                ),
            };
            response(
                "200 OK",
                "Content-Type: application/json\r\n",
                body.as_bytes(),
            )
        });
        let rpc = HttpJsonRpc::new(url, HttpTransport::new(1000, 1024, None).unwrap()).unwrap();
        let result = rpc.call("eth_chainId", json!([])).await;
        if mode == 0 {
            assert_eq!(result.unwrap(), json!("0x4cef52"));
        } else {
            assert!(!result.unwrap_err().to_string().contains("secret"));
        }
        worker.join().unwrap();
    }
}
#[tokio::test]
async fn forbidden_endpoints_methods_and_diagnostics() {
    for endpoint in [
        "http://user:secret@localhost",
        "https://host/../secret",
        "https://host/%2e%2e/secret",
        "https://host/#secret",
        "https://host/\\secret",
        "file:///secret",
    ] {
        let err =
            HttpJsonRpc::new(endpoint, HttpTransport::new(100, 1024, None).unwrap()).unwrap_err();
        assert!(!format!("{err:?} {err}").contains("secret"));
    }
    let rpc = HttpJsonRpc::new(
        "http://127.0.0.1:1/private-token",
        HttpTransport::new(100, 1024, None).unwrap(),
    )
    .unwrap();
    assert!(!format!("{rpc:?}").contains("private-token"));
    assert!(rpc.call("eth_sendRawTransaction", json!([])).await.is_err());
    assert!(rpc.call("eth_chainId", json!({})).await.is_err());
    assert!(rpc.call("eth_chainId", json!([])).await.is_err());
    assert!(HttpTransport::new(0, 1024, None).is_err());
    assert!(HttpTransport::new(100, 0, None).is_err());
    assert!(HttpTransport::new(100, 1024, Some(b"not a certificate")).is_err());
}
#[tokio::test]
async fn response_deadline_includes_waiting_for_body() {
    let (url, _requests, worker) = server(|_| {
        thread::sleep(Duration::from_millis(150));
        response("200 OK", "Content-Type: application/json\r\n", b"{}")
    });
    assert!(
        HttpTransport::new(30, 1024, None)
            .unwrap()
            .request(&url, None)
            .await
            .is_err()
    );
    worker.join().unwrap();
}

/// Invoked only by the public Python integration fixture with an owned TLS peer.
#[tokio::test]
#[ignore = "requires the owned TLS fixture in test_rust_boundaries.py"]
async fn verified_tls_fixture() {
    let port = std::env::var("MPPI_TLS_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let pem = std::fs::read(std::env::var("MPPI_TLS_TEST_CA").unwrap()).unwrap();
    let http = HttpTransport::new(1000, 1024, Some(&pem)).unwrap();
    assert_eq!(
        http.request(&format!("https://localhost:{port}/ok"), None)
            .await
            .unwrap(),
        b"{\"ok\":true}"
    );
    assert!(
        http.request(&format!("https://127.0.0.1:{port}/ok"), None)
            .await
            .is_err()
    );
    assert!(
        HttpTransport::new(1000, 1024, None)
            .unwrap()
            .request(&format!("https://localhost:{port}/ok"), None)
            .await
            .is_err()
    );
}
