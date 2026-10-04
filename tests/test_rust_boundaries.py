"""Rust TLS checks against an owned local peer with temporary test certificates."""

from __future__ import annotations

import os
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

RUST = Path(__file__).resolve().parents[1] / "sdks/provider/rust"


class JsonPeer(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def test_rust_verifies_tls_roots_and_hostname(tmp_path: Path) -> None:
    """Use a CA and a separate server leaf, matching ordinary verified TLS."""

    def openssl(*arguments: str) -> None:
        subprocess.run(
            ["openssl", *arguments], cwd=tmp_path, check=True, capture_output=True, timeout=15
        )

    openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
        "-days",
        "1",
        "-subj",
        "/CN=MPPI temporary test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
    )
    openssl(
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "leaf.key",
        "-out",
        "leaf.csr",
        "-subj",
        "/CN=localhost",
    )
    (tmp_path / "leaf.ext").write_text(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n"
    )
    openssl(
        "x509",
        "-req",
        "-in",
        "leaf.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        "leaf.pem",
        "-days",
        "1",
        "-extfile",
        "leaf.ext",
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "leaf.pem", tmp_path / "leaf.key")
    peer = ThreadingHTTPServer(("127.0.0.1", 0), JsonPeer)
    peer.socket = context.wrap_socket(peer.socket, server_side=True)
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    try:
        subprocess.run(
            [
                "cargo",
                "test",
                "--locked",
                "--test",
                "http_transport",
                "--",
                "--ignored",
                "--exact",
                "verified_tls_fixture",
            ],
            cwd=RUST,
            env={
                **os.environ,
                "MPPI_TLS_TEST_PORT": str(peer.server_address[1]),
                "MPPI_TLS_TEST_CA": str(tmp_path / "ca.pem"),
            },
            check=True,
            timeout=300,
        )
    finally:
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=5)
