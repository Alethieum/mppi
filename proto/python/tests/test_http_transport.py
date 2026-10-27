"""Real HTTP/TLS tests; credentials exist only in a temporary test directory."""

import calendar
import json
import math
import ssl
import subprocess
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from eth_abi.abi import decode, encode
from mppi_protocol import chain
from mppi_protocol.catalog import DEFAULT_CATALOG_BYTES, parse_catalog
from mppi_protocol.chain import ChainEvidenceError, HttpJsonRpc
from mppi_protocol.discovery import MAX_DOCUMENT_BYTES, parse_document
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.http import (
    MAX_RPC_RESPONSE_BYTES,
    HttpTransport,
    TransportError,
    _retry_after,
)


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
        if self.path.startswith(("/status/", "/flaky/", "/after/")):
            # /after/<status>/<failures>/<Retry-After name>: refusals carry that field.
            _, kind, status, failures, after = (self.path + "/0/none").split("/")[:5]
            with ATTEMPTS_LOCK:
                ATTEMPTS[self.path] = ATTEMPTS.get(self.path, 0) + 1
                failing = kind == "status" or ATTEMPTS[self.path] <= int(failures)
            if failing:
                body = b'{"error":"remote secret value"}'
                self.send_response(int(status))
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for value in RETRY_AFTER.get(after, []):
                    self.send_header("Retry-After", value)
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
RETRY_AFTER = {
    "seconds": ["2"],
    "padded": ["7 \t"],
    "long": ["30"],
    "past": ["Fri, 31 Dec 1999 23:59:59 GMT"],
    "twice": ["2", "3"],
    "invalid": ["soon"],
}


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


def test_rpc_read_retries_rate_limits_with_bounded_jittered_backoff(monkeypatch):
    # Full jitter: each repeat waits a uniform share of its 250 ms or 1 s ceiling.
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    shares = iter([0.5, 0.25, 0.0, 1.0])
    monkeypatch.setattr(chain, "_jitter", lambda: next(shares))
    with server() as port:
        endpoint = f"http://127.0.0.1:{port}"
        assert HttpJsonRpc(endpoint + "/flaky/429/2").call("eth_chainId", []) == "0x4cef52"
        assert ATTEMPTS["/flaky/429/2"] == 3 and delays == [0.125, 0.25]
        delays.clear()
        with pytest.raises(TransportError) as exhausted:
            HttpJsonRpc(endpoint + "/flaky/503/9").call("eth_chainId", [])
        assert exhausted.value.transient and ATTEMPTS["/flaky/503/9"] == 3
        assert delays == [0.0, 1.0]
        delays.clear()
        with pytest.raises(TransportError):
            HttpJsonRpc(endpoint + "/flaky/404/1", HttpTransport(timeout_ms=200)).call(
                "eth_chainId", []
            )
        assert ATTEMPTS["/flaky/404/1"] == 1 and delays == []


@pytest.mark.parametrize(
    ("after", "wait"),
    [
        ("seconds", 2.0),
        ("padded", 7.0),
        ("past", 0.0),
        ("long", 30.0),
        ("twice", None),
        ("invalid", None),
        ("none", None),
    ],
)
def test_a_refusal_keeps_the_wait_its_retry_after_asks_for(after, wait):
    with server() as port:
        with pytest.raises(TransportError) as caught:
            HttpTransport(timeout_ms=1000).request(
                f"http://127.0.0.1:{port}/after/503/1/{after}", body=b'{"id":1}'
            )
    assert caught.value.transient and caught.value.retry_after == wait


# Sun, 06 Nov 1994 08:49:30 GMT: seven seconds before RFC 9110's example date.
EXAMPLE_NOW = calendar.timegm((1994, 11, 6, 8, 49, 30))


@pytest.mark.parametrize(
    ("value", "wait"),
    [
        ("120", 120.0),
        ("0", 0.0),
        ("0012", 12.0),
        pytest.param("9" * 400, math.inf, id="400-digits"),
        ("Sun, 06 Nov 1994 08:49:37 GMT", 7.0),  # IMF-fixdate
        ("Sunday, 06-Nov-94 08:49:37 GMT", 7.0),  # obsolete RFC 850 format
        ("Sun Nov  6 08:49:37 1994", 7.0),  # ANSI C asctime()
        ("Sun Nov 06 08:49:37 1994", 7.0),
        ("Sun, 06 Nov 1994 08:49:00 GMT", 0.0),  # Already past: no wait.
        ("Sun, 06 Nov 1994 08:50:60 GMT", 90.0),  # A leap second.
        ("Thu, 29 Feb 1996 00:00:00 GMT", calendar.timegm((1996, 2, 29, 0, 0, 0)) - EXAMPLE_NOW),
        ("1.5", None),
        ("-1", None),
        ("+1", None),
        ("", None),
        ("\u0661\u0662", None),  # Not ASCII digits.
        ("sun, 06 nov 1994 08:49:37 gmt", None),  # HTTP-date is case sensitive.
        ("Sun, 6 Nov 1994 08:49:37 GMT", None),
        ("Sun,  06 Nov 1994 08:49:37 GMT", None),
        ("Sun, 06 Nov 1994 08:49:37 UTC", None),
        ("Sun, 31 Nov 1994 08:49:37 GMT", None),
        ("Fri, 29 Feb 1995 00:00:00 GMT", None),
        ("Sun, 06 Nov 1994 24:00:00 GMT", None),
        ("Sun, 06 Nov 1994 08:60:00 GMT", None),
        ("Sun Nov 6 08:49:37 1994", None),
        ("Sun, 06-Nov-94 08:49:37 GMT", None),
        ("Sunday, 06 Nov 1994 08:49:37 GMT", None),
    ],
)
def test_retry_after_accepts_delay_seconds_and_each_http_date_format(value, wait):
    # RFC 9110 section 10.2.3; a recipient must accept all three HTTP-date
    # formats (section 5.6.7).
    assert _retry_after(value, EXAMPLE_NOW) == wait


def test_a_two_digit_year_more_than_fifty_years_ahead_is_the_century_before():
    # RFC 9110 section 5.6.7, for rfc850-date.
    late_2099 = calendar.timegm((2099, 12, 31, 23, 59, 50))
    assert _retry_after("Friday, 01-Jan-00 00:00:00 GMT", late_2099) == 10.0
    now = calendar.timegm((2026, 10, 27, 0, 0, 0))
    fifty_years = calendar.timegm((2076, 10, 27, 0, 0, 0)) - now
    assert _retry_after("Tuesday, 27-Oct-76 00:00:00 GMT", now) == fifty_years
    assert _retry_after("Wednesday, 28-Oct-76 00:00:00 GMT", now) == 0.0  # 1976.
    assert _retry_after("Monday, 26-Oct-26 00:00:00 GMT", now) == 0.0


def test_a_read_waits_out_retry_after_then_its_jittered_delay(monkeypatch):
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    monkeypatch.setattr(chain, "_jitter", lambda: 0.5)
    with server() as port:
        endpoint = f"http://127.0.0.1:{port}"
        assert HttpJsonRpc(endpoint + "/after/429/1/seconds").call("eth_chainId", []) == (
            "0x4cef52"
        )
        assert ATTEMPTS["/after/429/1/seconds"] == 2 and delays == [2.125]
        delays.clear()
        # A wait that cannot end before the read's repeats must start is not
        # waited: the read fails at once. Attempts are 5 s, repeats start within 6.25 s.
        with pytest.raises(TransportError) as refused:
            HttpJsonRpc(endpoint + "/after/429/1/long").call("eth_chainId", [])
        assert refused.value.retry_after == 30.0
        assert ATTEMPTS["/after/429/1/long"] == 1 and delays == []


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
        size = int(self.headers["Content-Length"])
        payload = json.loads(self.rfile.read(size))
        with KEPT_LOCK:
            KEPT["requests"] += 1
            KEPT["bodies"].append(payload)
            KEPT["sizes"].append(size)
            stall = self.path == "/stall-once" and KEPT["requests"] == 1
        if stall:
            threading.Event().wait(0.6)  # Longer than the client waits.
        if self.path == "/refuse" or (isinstance(payload, list) and self.path == "/refuse-batch"):
            self.send_response(400)  # As a gateway that accepts no batch requests.
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if isinstance(payload, list) and self.path in ("/single", "/single-limited"):
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
        value = call["params"][0]
        if self.path in ("/limited", "/single-limited") and value.startswith("limited"):
            # Each such call is refused once, as a per-second limit refuses some
            # entries of a batch (QuickNode -32007).
            with KEPT_LOCK:
                limited = value not in KEPT["limited"]
                KEPT["limited"].add(value)
            if limited:
                return {"jsonrpc": "2.0", "id": call["id"], "error": {"code": -32007}}
        return {"jsonrpc": "2.0", "id": call["id"], "result": value}


KEPT: dict = {"connections": 0, "requests": 0, "limited": set(), "bodies": [], "sizes": []}
KEPT_LOCK = threading.Lock()


@contextmanager
def kept_alive():
    with KEPT_LOCK:
        KEPT.update(connections=0, requests=0, limited=set(), bodies=[], sizes=[])
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


def test_no_request_body_exceeds_the_transport_bound():
    # 1 MiB: a longer body is refused before anything is sent (port 9 is never
    # contacted, so the refusal is not a transient connection failure).
    with pytest.raises(TransportError) as refused:
        HttpTransport().request("http://127.0.0.1:9/", body=b" " * (MAX_DOCUMENT_BYTES + 1))
    assert not refused.value.transient
    call = ("eth_call", [{"to": "0x" + "ca11" * 10, "data": "0x" + "00" * 49_500}, "latest"])
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint)
        # Twelve calls of about 99 KB each: two batches, never one over 1 MiB.
        assert len(rpc.call_many([call] * 12)) == 12
        assert KEPT["requests"] == 2 and max(KEPT["sizes"]) <= MAX_DOCUMENT_BYTES
        # A call that cannot fit one body is refused before anything is sent.
        oversized = ("eth_call", [{"to": "0x" + "ca11" * 10, "data": "0x" + "00" * (1 << 19)}])
        with pytest.raises(TransportError) as refused:
            rpc.call_many([oversized])
        assert not refused.value.transient and KEPT["requests"] == 2
        assert len(rpc.call_many([call] * 2)) == 2  # Batches stay on.
        assert KEPT["requests"] == 3
        rpc.close()


def sent(body):
    """The first parameter of each call in a request body, in order."""
    return [call["params"][0] for call in body] if isinstance(body, list) else body["params"][0]


def test_only_the_refused_entries_of_a_batch_are_sent_again(monkeypatch):
    # JSON-RPC 2.0 section 6 matches each answer to its call by id, so the
    # entries a batch answered are kept and only the refused ones repeat.
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    monkeypatch.setattr(chain, "_jitter", lambda: 1.0)
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/limited")
        calls = [
            ("eth_getBlockByNumber", [value, False])
            for value in ("limited-a", "0x1", "limited-b", "0x2")
        ]
        assert rpc.call_many(calls) == ["limited-a", "0x1", "limited-b", "0x2"]
        calls = [("eth_getBlockByNumber", [value, False]) for value in ("0x3", "limited-c")]
        assert rpc.call_many(calls) == ["0x3", "limited-c"]
        rpc.close()
    assert [sent(body) for body in KEPT["bodies"]] == [
        ["limited-a", "0x1", "limited-b", "0x2"],
        ["limited-a", "limited-b"],
        ["0x3", "limited-c"],
        "limited-c",  # One refused entry is sent again as a single call.
    ]
    assert delays == [0.25, 0.25]


def test_one_call_at_a_time_a_refused_call_repeats_without_those_answered(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)
    with kept_alive() as endpoint:
        rpc = HttpJsonRpc(endpoint + "/single-limited")
        calls = [("eth_getBlockByNumber", [value, False]) for value in ("0x0", "limited", "0x2")]
        assert rpc.call_many(calls) == ["0x0", "limited", "0x2"]
        rpc.close()
    assert [sent(body) for body in KEPT["bodies"]] == [
        ["0x0", "limited", "0x2"],  # Refused as a batch: then one call at a time.
        "0x0",
        "limited",
        "limited",
        "0x2",
    ]


def test_a_definite_answer_in_a_batch_ends_the_read_even_beside_a_refusal(monkeypatch):
    delays = []
    monkeypatch.setattr(time, "sleep", delays.append)
    requests = []

    def reply(endpoint, *, body):
        sent = json.loads(body)
        requests.append(sent)
        return json.dumps(
            [
                {"jsonrpc": "2.0", "id": sent[0]["id"], "error": {"code": -32005}},
                {"jsonrpc": "2.0", "id": sent[1]["id"], "error": {"code": -32602}},
            ]
        ).encode()

    monkeypatch.setattr(
        HttpTransport, "request", lambda self, url, *, body=None: reply(url, body=body)
    )
    with pytest.raises(ChainEvidenceError, match="no usable result"):
        HttpJsonRpc("http://127.0.0.1:9").call_many(
            [("eth_chainId", []), ("eth_getBlockByNumber", ["latest", False])]
        )
    assert len(requests) == 1 and delays == []


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
