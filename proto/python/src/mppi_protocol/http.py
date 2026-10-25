"""Small bounded HTTP transport for discovery and chain JSON-RPC.

No redirects, cookies, implicit proxy credentials, retries or wallet submissions;
failures say whether they are transient so a read-only caller may repeat them.
Run blocking reads away from the gRPC I/O loop. TLS uses certificate/hostname
verification; an optional CA file adds support for provider-managed private PKI.
"""

from __future__ import annotations

import http.client
import ssl
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .discovery import MAX_DOCUMENT_BYTES
from .errors import ValidationCode, ValidationError

MAX_RPC_RESPONSE_BYTES = 4 << 20


# Statuses a later identical read may not repeat: timeout, too early, rate limit
# and server/gateway unavailability (RFC 9110 sections 15.5.9, 15.6; RFC 8470
# section 5.2; RFC 6585 section 4).
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class TransportError(RuntimeError):
    """Read did not produce usable evidence; never proof of economic failure.

    ``transient`` marks a rate limit, an unavailable server or a dropped
    connection, which a later identical read may not repeat. A timeout,
    rejected content, TLS verification failure or other status is not.
    """

    def __init__(self, message: str, *, transient: bool = False) -> None:
        super().__init__(message)
        self.transient = transient


@dataclass(frozen=True, slots=True)
class HttpTransport:
    timeout_ms: int = 10_000
    max_bytes: int = MAX_DOCUMENT_BYTES
    ca_file: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if type(self.timeout_ms) is not int or not 0 < self.timeout_ms <= 86_400_000:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "HTTP timeout must be 1..86400000 ms"
            )
        if type(self.max_bytes) is not int or not 0 < self.max_bytes <= MAX_RPC_RESPONSE_BYTES:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "HTTP byte limit must be 1..4194304"
            )

    def request(self, url: str, *, body: bytes | None = None) -> bytes:
        if type(url) is not str or any(ord(c) <= 32 or ord(c) >= 127 for c in url):
            raise TransportError("invalid HTTP endpoint")
        try:
            parsed = urlsplit(url)
            host, port = parsed.hostname, parsed.port
        except ValueError:
            raise TransportError("invalid HTTP endpoint") from None
        if (
            parsed.scheme not in ("https", "http")
            or not host
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or "\\" in url
        ):
            raise TransportError("invalid HTTP endpoint")
        # Plain HTTP is useful for an explicitly configured private/local RPC.
        # Discovery callers additionally require normalize_origin's HTTPS rule.
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        connection: http.client.HTTPConnection
        try:
            if parsed.scheme == "https":
                context = ssl.create_default_context(cafile=self.ca_file)
                context.set_alpn_protocols(["http/1.1"])
                connection = http.client.HTTPSConnection(
                    host, port, timeout=self.timeout_ms / 1000, context=context
                )
            else:
                connection = http.client.HTTPConnection(host, port, timeout=self.timeout_ms / 1000)
        except (OSError, ValueError):
            raise TransportError("HTTP transport configuration failed") from None
        try:
            connection.request(
                "GET" if body is None else "POST",
                path,
                body=body,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Content-Type": "application/json",
                },
            )
            response = connection.getresponse()
            if response.status != 200:
                raise TransportError(
                    f"HTTP endpoint returned status {response.status}",
                    transient=response.status in TRANSIENT_STATUSES,
                )
            if response.getheader("Content-Encoding", "identity").lower() != "identity":
                raise TransportError("compressed HTTP response is not accepted")
            if (
                response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise TransportError("HTTP endpoint did not return application/json")
            raw = response.read(self.max_bytes + 1)
            if len(raw) > self.max_bytes:
                raise TransportError("HTTP response exceeds the configured byte limit")
            return raw
        except (TimeoutError, ssl.SSLCertVerificationError, ValueError):
            # Do not echo endpoint credentials, remote bodies or TLS file paths.
            raise TransportError("HTTP request failed") from None
        except (OSError, http.client.HTTPException):
            # Refused, reset or dropped connections, including a closed keep-alive.
            raise TransportError("HTTP request failed", transient=True) from None
        finally:
            connection.close()
