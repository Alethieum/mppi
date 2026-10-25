"""Real HTTP/TLS tests; credentials exist only in a temporary test directory."""

import json
import ssl
import subprocess
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from eth_abi.abi import decode, encode
from mppi_protocol.catalog import DEFAULT_CATALOG_BYTES, parse_catalog
from mppi_protocol.chain import ChainEvidenceError, HttpJsonRpc
from mppi_protocol.discovery import MAX_DOCUMENT_BYTES, parse_document
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.http import MAX_RPC_RESPONSE_BYTES, HttpTransport, TransportError


def catalog_at_size(size):
    # The protocol caps the repository component; the namespace remains exact
    # ASCII. This makes one valid canonical row reach the public catalog bound.
    prefix = b'[{"input":"0","model":"'
    suffix = b'/b","output":"0"}]'
    return prefix + b"a" * (size - len(prefix) - len(suffix)) + suffix


class Peer(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        status = 302 if self.path == "/redirect" else 200
        body = b"x" * 50 if self.path == "/large" else b'{"ok":true}'
        if self.path.startswith("/document/"):
            size = int(self.path.removeprefix("/document/"))
            body += b" " * (size - len(body))
        self.send_response(status)
        self.send_header(
            "Content-Type", "text/html" if self.path == "/html" else "application/json"
        )
        self.send_header("Content-Length", str(len(body)))
        if status == 302:
            self.send_header("Location", "/ok")
        if self.path == "/compressed":
            self.send_header("Content-Encoding", "gzip")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        response = {"jsonrpc": "2.0", "id": request["id"], "result": "0x4cef52"}
        if self.path.startswith("/slow"):
            threading.Event().wait(1)
        if self.path.startswith(("/status/", "/flaky/")):
            _, kind, status, failures = (self.path + "/0").split("/")[:4]
            with ATTEMPTS_LOCK:
                ATTEMPTS[self.path] = ATTEMPTS.get(self.path, 0) + 1
                failing = kind == "status" or ATTEMPTS[self.path] <= int(failures)
            if failing:
                body = b'{"error":"remote secret value"}'
                self.send_response(int(status))
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
        if self.path == "/wrong-id":
            response["id"] = "wrong"
        if self.path == "/error":
            response["error"] = {"message": "remote secret value"}
        if self.path.startswith("/catalog/"):
            catalog = catalog_at_size(int(self.path.removeprefix("/catalog/")))
            response["result"] = "0x" + encode(["bytes"], [catalog]).hex()
        body = json.dumps(response).encode()
        if self.path.startswith("/rpc-size/"):
            size = int(self.path.removeprefix("/rpc-size/"))
            body += b" " * (size - len(body))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ATTEMPTS: dict[str, int] = {}
ATTEMPTS_LOCK = threading.Lock()


@contextmanager
def server(context=None):
    peer = ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    if context:
        peer.socket = context.wrap_socket(peer.socket, server_side=True)
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    try:
        yield peer.server_address[1]
    finally:
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=5)


def test_http_limits_redirect_rejection_and_rpc_correlation():
    with server() as port:
        endpoint = f"http://127.0.0.1:{port}"
        http = HttpTransport(timeout_ms=1000, max_bytes=20)
        assert http.request(endpoint + "/ok") == b'{"ok":true}'
        for path in ("/large", "/redirect", "/html", "/compressed"):
            with pytest.raises(TransportError):
                http.request(endpoint + path)
        assert HttpJsonRpc(endpoint).call("eth_chainId", []) == "0x4cef52"
        for path in ("/wrong-id", "/error"):
            with pytest.raises(ChainEvidenceError) as failure:
                HttpJsonRpc(endpoint + path).call("eth_chainId", [])
            assert "secret" not in str(failure.value)
        with pytest.raises(ChainEvidenceError):
            HttpJsonRpc(endpoint).call("eth_sendTransaction", [])


@pytest.mark.parametrize("size", [(512 << 10) + 1, DEFAULT_CATALOG_BYTES])
def test_rpc_reads_canonical_catalog_through_abi_and_json_hex_expansion(size):
    catalog = catalog_at_size(size)
    assert len(catalog) == size
    assert parse_catalog(catalog).canonical_bytes == catalog
    with server() as port:
        result = HttpJsonRpc(f"http://127.0.0.1:{port}/catalog/{size}").call(
            "eth_call", [{"to": "0x" + "01" * 20, "data": "0x"}, "latest"]
        )
    assert isinstance(result, str) and result.startswith("0x")
    received = decode(["bytes"], bytes.fromhex(result[2:]))[0]
    assert received == catalog
    assert parse_catalog(received).canonical_bytes == catalog


def test_rpc_response_limit_is_four_mib_and_discovery_limit_remains_one_mib():
    assert MAX_RPC_RESPONSE_BYTES == 4 << 20
    assert HttpTransport().max_bytes == MAX_DOCUMENT_BYTES == 1 << 20
    with pytest.raises(ValidationError):
        HttpTransport(max_bytes=MAX_RPC_RESPONSE_BYTES + 1)
    with pytest.raises(ValidationError):
        HttpTransport(max_bytes=True)
    with server() as port:
        endpoint = f"http://127.0.0.1:{port}"
        assert (
            HttpJsonRpc(endpoint + f"/rpc-size/{MAX_RPC_RESPONSE_BYTES}").call("eth_chainId", [])
            == "0x4cef52"
        )
        with pytest.raises(TransportError):
            HttpJsonRpc(endpoint + f"/rpc-size/{MAX_RPC_RESPONSE_BYTES + 1}").call(
                "eth_chainId", []
            )
        raw = HttpTransport().request(endpoint + f"/document/{MAX_DOCUMENT_BYTES}")
        assert len(raw) == MAX_DOCUMENT_BYTES
        assert parse_document(raw) == {"ok": True}
        with pytest.raises(TransportError):
            HttpTransport().request(endpoint + f"/document/{MAX_DOCUMENT_BYTES + 1}")
    with pytest.raises(ValidationError) as oversized:
        parse_document(raw + b" ")
    assert oversized.value.code is ValidationCode.LIMIT_EXCEEDED
    with pytest.raises(ValidationError):
        parse_document(b"{}", max_bytes=MAX_RPC_RESPONSE_BYTES)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"jsonrpc":"2.0","id":"ID","result":{"a":1,"\\u0061":2}}',
        b'{"jsonrpc":"2.0","id":"ID","result":NaN}',
        b'{"jsonrpc":"2.0","id":"ID","result":"\xff"}',
        b'\xef\xbb\xbf{"jsonrpc":"2.0","id":"ID","result":0}',
        b'{"jsonrpc":"2.0","id":"ID","result":' + b"[" * 128 + b"0" + b"]" * 128 + b"}",
        b"[]",
    ],
)
def test_rpc_parser_retains_strict_json_guards(monkeypatch, raw):
    def response(self, url, *, body):
        request_id = json.loads(body)["id"]
        return raw.replace(b'"ID"', json.dumps(request_id).encode())

    monkeypatch.setattr(HttpTransport, "request", response)
    with pytest.raises(ValidationError):
        HttpJsonRpc("http://unused.test").call("eth_chainId", [])


def test_rpc_parser_checks_its_own_bound_and_preserves_scalar_types(monkeypatch):
    def response(self, url, *, body):
        request_id = json.loads(body)["id"]
        return json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "result": [9007199254740993, False, None, "7"]}
        ).encode()

    monkeypatch.setattr(HttpTransport, "request", response)
    result = HttpJsonRpc("http://unused.test").call("eth_chainId", [])
    assert result == [9007199254740993, False, None, "7"]

    def oversized(self, url, *, body):
        return b" " * (MAX_RPC_RESPONSE_BYTES + 1)

    monkeypatch.setattr(HttpTransport, "request", oversized)
    with pytest.raises(ValidationError) as failure:
        HttpJsonRpc("http://unused.test").call("eth_chainId", [])
    assert failure.value.code is ValidationCode.LIMIT_EXCEEDED


def test_verified_tls_custom_ca_and_hostname_failure(tmp_path):
    key = tmp_path / "key.pem"
    cert = tmp_path / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    with server(context) as port:
        http = HttpTransport(timeout_ms=1000, ca_file=str(cert))
        assert http.request(f"https://localhost:{port}/ok") == b'{"ok":true}'
        with pytest.raises(TransportError):
            http.request(f"https://127.0.0.1:{port}/ok")
        with pytest.raises(TransportError):
            HttpTransport(timeout_ms=1000).request(f"https://localhost:{port}/ok")


def test_secrets_do_not_appear_in_transport_representation_or_errors():
    endpoint = "http://user:secret@localhost"
    assert "secret" not in repr(HttpJsonRpc(endpoint))
    assert "secret" not in repr(HttpTransport(ca_file="secret.pem"))
    with pytest.raises(TransportError) as failure:
        HttpTransport().request(endpoint)
    assert "secret" not in str(failure.value)


@pytest.mark.parametrize(
    ("status", "transient"),
    [(408, True), (425, True), (429, True), (500, True), (502, True), (503, True), (504, True)]
    + [(400, False), (401, False), (403, False), (404, False), (413, False)],
)
def test_transport_marks_only_retryable_http_statuses_transient(status, transient):
    with server() as port:
        with pytest.raises(TransportError) as caught:
            HttpTransport(timeout_ms=1000).request(
                f"http://127.0.0.1:{port}/status/{status}", body=b'{"id":1}'
            )
    assert caught.value.transient is transient
    assert "secret" not in str(caught.value)


def test_transport_marks_dropped_connections_but_not_timeouts_or_content_transient():
    with server() as port:
        with pytest.raises(TransportError) as slow:
            HttpTransport(timeout_ms=200).request(f"http://127.0.0.1:{port}/slow", body=b'{"id":1}')
        with pytest.raises(TransportError) as html:
            HttpTransport(timeout_ms=1000).request(f"http://127.0.0.1:{port}/html")
    with pytest.raises(TransportError) as refused:
        HttpTransport(timeout_ms=1000).request(f"http://127.0.0.1:{port}/", body=b'{"id":1}')
    assert (slow.value.transient, html.value.transient, refused.value.transient) == (
        False,
        False,
        True,
    )


def test_rpc_read_retries_rate_limits_with_bounded_backoff(monkeypatch):
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    with server() as port:
        endpoint = f"http://127.0.0.1:{port}"
        assert HttpJsonRpc(endpoint + "/flaky/429/2").call("eth_chainId", []) == "0x4cef52"
        assert ATTEMPTS["/flaky/429/2"] == 3 and delays == [0.25, 1.0]
        delays.clear()
        with pytest.raises(TransportError) as exhausted:
            HttpJsonRpc(endpoint + "/flaky/503/9").call("eth_chainId", [])
        assert exhausted.value.transient and ATTEMPTS["/flaky/503/9"] == 3
        assert delays == [0.25, 1.0]
        delays.clear()
        for path in ("/flaky/404/1", "/slow/a"):
            with pytest.raises(TransportError):
                HttpJsonRpc(endpoint + path, HttpTransport(timeout_ms=200)).call("eth_chainId", [])
        assert ATTEMPTS["/flaky/404/1"] == 1 and delays == []
