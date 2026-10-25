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


def test_transport_marks_dropped_connections_and_timeouts_but_not_content_transient():
    with server() as port:
        with pytest.raises(TransportError) as slow:
            HttpTransport(timeout_ms=200).request(f"http://127.0.0.1:{port}/slow", body=b'{"id":1}')
        with pytest.raises(TransportError) as html:
            HttpTransport(timeout_ms=1000).request(f"http://127.0.0.1:{port}/html")
    with pytest.raises(TransportError) as refused:
        HttpTransport(timeout_ms=1000).request(f"http://127.0.0.1:{port}/", body=b'{"id":1}')
    assert (slow.value.transient, html.value.transient, refused.value.transient) == (
        True,
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
        with pytest.raises(TransportError):
            HttpJsonRpc(endpoint + "/flaky/404/1", HttpTransport(timeout_ms=200)).call(
                "eth_chainId", []
            )
        assert ATTEMPTS["/flaky/404/1"] == 1 and delays == []


class KeptAlive(BaseHTTPRequestHandler):
    """An HTTP/1.1 JSON-RPC peer that keeps connections open and answers batches.

    A batch's responses come back in reverse order: JSON-RPC 2.0 section 6 lets
    a server answer in any order, so the client must match them by id.
    """

    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def setup(self):
        super().setup()
        with KEPT_LOCK:
            KEPT["connections"] += 1

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with KEPT_LOCK:
            KEPT["requests"] += 1
            stall = self.path == "/stall-once" and KEPT["requests"] == 1
        if stall:
            threading.Event().wait(0.6)  # Longer than the client waits.
        if self.path == "/refuse" or (isinstance(payload, list) and self.path == "/refuse-batch"):
            self.send_response(400)  # As a gateway that accepts no batch requests.
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if isinstance(payload, list) and self.path == "/single":
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32600}}
        elif isinstance(payload, list):
            response = [self.answer(call) for call in reversed(payload)]
        else:
            response = self.answer(payload)
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        if self.path == "/drop":
            self.close_connection = True  # Closed while idle, without saying so.

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except BrokenPipeError:
            self.close_connection = True  # The client gave up on a stalled reply.

    def answer(self, call):
        if self.path == "/limited" and call["params"][0] == "limited":
            with KEPT_LOCK:
                KEPT["limited"] += 1
                limited = KEPT["limited"] == 1
            if limited:
                return {"jsonrpc": "2.0", "id": call["id"], "error": {"code": -32007}}
        return {"jsonrpc": "2.0", "id": call["id"], "result": call["params"][0]}


KEPT = {"connections": 0, "requests": 0, "limited": 0}
KEPT_LOCK = threading.Lock()


@contextmanager
def kept_alive():
    with KEPT_LOCK:
        KEPT.update(connections=0, requests=0, limited=0)
    peer = ThreadingHTTPServer(("127.0.0.1", 0), KeptAlive)
    peer.daemon_threads = True
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{peer.server_address[1]}"
    finally:
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=5)


def test_sequential_reads_reuse_one_kept_alive_connection():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint)
        for number in range(20):
            assert rpc.call("eth_getBlockByNumber", [hex(number), False]) == hex(number)
        rpc.close()
    assert (KEPT["requests"], KEPT["connections"]) == (20, 1)


def test_a_connection_the_server_closed_while_idle_is_replaced_once():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/drop")
        for number in range(3):
            assert rpc.call("eth_getBlockByNumber", [hex(number), False]) == hex(number)
        rpc.close()
    assert (KEPT["requests"], KEPT["connections"]) == (3, 3)


def test_a_batch_is_one_round_trip_and_matched_by_id():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint)
        calls = [("eth_getBlockByNumber", [hex(number), False]) for number in range(40)]
        assert rpc.call_many(calls) == [hex(number) for number in range(40)]
        rpc.close()
    # At most 32 calls per batch: a plan that counts each call stays within its limit.
    assert (KEPT["requests"], KEPT["connections"]) == (2, 1)


def test_a_rate_limited_entry_repeats_its_batch(monkeypatch):
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/limited")
        calls = [("eth_getBlockByNumber", ["limited", False]), ("eth_chainId", ["0x1"])]
        assert rpc.call_many(calls) == ["limited", "0x1"]
        rpc.close()
    assert KEPT["requests"] == 2 and delays == [0.25]


def test_an_endpoint_without_batches_is_read_one_call_at_a_time():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/single")
        calls = [("eth_getBlockByNumber", [hex(number), False]) for number in range(3)]
        assert rpc.call_many(calls) == ["0x0", "0x1", "0x2"]
        assert rpc.call_many(calls) == ["0x0", "0x1", "0x2"]
        rpc.close()
    assert KEPT["requests"] == 1 + 3 + 3  # One refused batch, then single calls.


def test_an_endpoint_that_refuses_batch_requests_is_read_one_call_at_a_time():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/refuse-batch")
        calls = [("eth_getBlockByNumber", [hex(number), False]) for number in range(3)]
        assert rpc.call_many(calls) == ["0x0", "0x1", "0x2"]
        assert rpc.call_many(calls) == ["0x0", "0x1", "0x2"]
        rpc.close()
    assert KEPT["requests"] == 1 + 3 + 3  # One refused batch, then single calls.


def test_an_endpoint_that_refuses_single_calls_too_keeps_its_batches():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/refuse")
        calls = [("eth_getBlockByNumber", [hex(number), False]) for number in range(3)]
        for _ in range(2):
            with pytest.raises(TransportError, match="status 400"):
                rpc.call_many(calls)
        rpc.close()
    assert KEPT["requests"] == 2 * (1 + 1)  # Each time a batch, then one single call.


@pytest.mark.parametrize("fault", ["missing", "duplicate", "foreign", "extra", "object"])
def test_a_batch_reply_must_answer_each_call_exactly_once(fault, monkeypatch):
    calls = [("eth_chainId", []), ("eth_getBlockByNumber", ["latest", False])]

    def reply(endpoint, *, body):
        sent = json.loads(body)
        rows = [{"jsonrpc": "2.0", "id": call["id"], "result": "0x1"} for call in sent]
        if fault == "missing":
            rows = rows[:1]
        elif fault == "duplicate":
            rows = [rows[0], rows[0]]
        elif fault == "foreign":
            rows[1]["id"] = "foreign"
        elif fault == "extra":
            rows.append({"jsonrpc": "2.0", "id": "extra", "result": "0x1"})
        else:
            rows = [rows[0], "0x1"]
        return json.dumps(rows).encode()

    transport = HttpTransport()
    monkeypatch.setattr(
        HttpTransport, "request", lambda self, url, *, body=None: reply(url, body=body)
    )
    with pytest.raises(ChainEvidenceError):
        HttpJsonRpc("http://127.0.0.1:9", transport).call_many(calls)


def test_a_batch_accepts_only_read_only_methods():
    with pytest.raises(ChainEvidenceError):
        HttpJsonRpc("http://127.0.0.1:9").call_many(
            [("eth_chainId", []), ("eth_sendRawTransaction", ["0x00"])]
        )


def test_a_read_that_times_out_once_is_repeated_and_answered():
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/stall-once", HttpTransport(timeout_ms=300))
        assert rpc.call("eth_getBlockByNumber", ["0x5", False]) == "0x5"
        rpc.close()
    assert KEPT["requests"] == 2
