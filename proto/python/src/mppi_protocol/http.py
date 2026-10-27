"""Small bounded HTTP transport for discovery and chain JSON-RPC.

No redirects, cookies, implicit proxy credentials or wallet submissions;
failures say whether they are transient so a read-only caller may repeat them.
A request body holds at most 1 MiB (MAX_DOCUMENT_BYTES); a longer one is
refused before anything is sent.
Run blocking reads away from the gRPC I/O loop. TLS uses certificate/hostname
verification; an optional CA file adds support for provider-managed private PKI.

HTTP/1.1 connections persist by default (RFC 9112 section 9.3), so each origin
keeps up to four idle connections for reuse, each for at most 30 s. A server
may close an idle connection at any time (RFC 9112 section 9.5). Only reads use
this transport, so a request that fails on a reused connection before any
response arrives is sent once more on a new connection, as RFC 9110 section
9.2.2 permits for a request known to be safe.
"""

from __future__ import annotations

import http.client
import re
import ssl
import threading
import time
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .discovery import MAX_DOCUMENT_BYTES
from .errors import ValidationCode, ValidationError

MAX_RPC_RESPONSE_BYTES = 4 << 20

_IDLE_PER_ORIGIN = 4
_IDLE_SECONDS = 30.0

# Statuses a later identical read may not repeat: timeout, too early, rate limit
# and server/gateway unavailability (RFC 9110 sections 15.5.9, 15.6; RFC 8470
# section 5.2; RFC 6585 section 4).
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

_Origin = tuple[str, str, int | None]

# Retry-After is delay-seconds or an HTTP-date (RFC 9110 section 10.2.3), and a
# recipient must accept each of the three HTTP-date formats (section 5.6.7).
# HTTP-date is case sensitive.
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_MONTH_DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
_MONTH = "(" + "|".join(_MONTHS) + ")"
_TIME = "([0-9]{2}):([0-9]{2}):([0-9]{2})"
_IMF_FIXDATE = re.compile(
    rf"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), ([0-9]{{2}}) {_MONTH} ([0-9]{{4}}) {_TIME} GMT"
)
_RFC850_DATE = re.compile(
    rf"(?:Mon|Tues|Wednes|Thurs|Fri|Satur|Sun)day, ([0-9]{{2}})-{_MONTH}-([0-9]{{2}}) {_TIME} GMT"
)
_ASCTIME_DATE = re.compile(
    rf"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) {_MONTH} ([0-9]{{2}}| [0-9]) {_TIME} ([0-9]{{4}})"
)


class TransportError(RuntimeError):
    """Read did not produce usable evidence; never proof of economic failure.

    ``transient`` marks a rate limit, an unavailable server, a dropped
    connection or a timeout, which a later identical read may not repeat:
    every request this transport sends is a read. Rejected content, TLS
    verification failure or another status is not transient.
    ``retry_after`` is the wait in seconds a refusing response's one
    Retry-After field asked for, or None.
    """

    def __init__(
        self, message: str, *, transient: bool = False, retry_after: float | None = None
    ) -> None:
        super().__init__(message)
        self.transient = transient
        self.retry_after = retry_after


def _days(year: int, month: int, day: int) -> int:
    """Days from 1970-01-01 to a proleptic Gregorian date (days_from_civil,
    https://howardhinnant.github.io/date_algorithms.html)."""
    year -= month <= 2
    era = year // 400
    of_era = year - era * 400
    of_year = (153 * (month - 3 if month > 2 else month + 9) + 2) // 5 + day - 1
    return era * 146_097 + of_era * 365 + of_era // 4 - of_era // 100 + of_year - 719_468


def _http_date(value: str, now: float) -> int | None:
    """An HTTP-date as Unix time, or None; ``now`` is Unix time."""
    if found := _IMF_FIXDATE.fullmatch(value):
        day_text, name, year_text, *clock = found.groups()
    elif found := _ASCTIME_DATE.fullmatch(value):
        name, day_text, *clock, year_text = found.groups()
    elif found := _RFC850_DATE.fullmatch(value):
        day_text, name, year_text, *clock = found.groups()
    else:
        return None
    year, month, day = int(year_text), _MONTHS.index(name) + 1, int(day_text)
    hour, minute, second = (int(part) for part in clock)
    seconds = hour * 3600 + minute * 60 + second
    if len(year_text) == 2:
        # rfc850-date: the latest year with these two digits whose date is not
        # more than 50 years after now.
        current = time.gmtime(now).tm_year
        year = current + 50 - (current + 50 - year) % 100
        if _days(year - 50, month, day) * 86_400 + seconds > now:
            year -= 100
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    if not 1 <= day <= _MONTH_DAYS[month - 1] + (leap and month == 2):
        return None
    if hour > 23 or minute > 59 or second > 60:
        return None
    return _days(year, month, day) * 86_400 + seconds


def _retry_after(value: str, now: float) -> float | None:
    """The seconds a Retry-After field value asks to wait from ``now`` (Unix
    time): its delay-seconds, or the time until its HTTP-date, none once that
    has passed. None for any other value."""
    value = value.strip(" \t")
    if re.fullmatch("[0-9]+", value):
        return float(value)
    at = _http_date(value, now)
    return None if at is None else max(0.0, at - now)


class _Stale(Exception):
    """A reused connection the server had already closed; nothing was answered."""


@dataclass(frozen=True, slots=True)
class HttpTransport:
    timeout_ms: int = 10_000
    max_bytes: int = MAX_DOCUMENT_BYTES
    ca_file: str | None = field(default=None, repr=False)
    _idle: dict[_Origin, list[tuple[http.client.HTTPConnection, float]]] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )

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
        if body is not None and len(body) > MAX_DOCUMENT_BYTES:
            raise TransportError("HTTP request body exceeds 1 MiB")
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
        origin = (parsed.scheme, host, port)
        idle = self._take(origin)
        if idle is not None:
            try:
                return self._exchange(origin, idle, path, body, reused=True)
            except _Stale:
                pass  # Closed by the server while idle: once more, on a new connection.
        return self._exchange(origin, self._connect(origin), path, body, reused=False)

    def close(self) -> None:
        """Close every idle connection; later requests open new ones."""
        with self._lock:
            idle = [connection for rows in self._idle.values() for connection, _ in rows]
            self._idle.clear()
        for connection in idle:
            connection.close()

    def _connect(self, origin: _Origin) -> http.client.HTTPConnection:
        scheme, host, port = origin
        try:
            if scheme == "https":
                context = ssl.create_default_context(cafile=self.ca_file)
                context.set_alpn_protocols(["http/1.1"])
                return http.client.HTTPSConnection(
                    host, port, timeout=self.timeout_ms / 1000, context=context
                )
            return http.client.HTTPConnection(host, port, timeout=self.timeout_ms / 1000)
        except (OSError, ValueError):
            raise TransportError("HTTP transport configuration failed") from None

    def _take(self, origin: _Origin) -> http.client.HTTPConnection | None:
        now = time.monotonic()
        expired = []
        with self._lock:
            rows = self._idle.get(origin, [])
            found = None
            while rows and found is None:
                connection, since = rows.pop()
                if now - since < _IDLE_SECONDS:
                    found = connection
                else:
                    expired.append(connection)
        for connection in expired:
            connection.close()
        return found

    def _keep(self, origin: _Origin, connection: http.client.HTTPConnection) -> None:
        with self._lock:
            rows = self._idle.setdefault(origin, [])
            if len(rows) < _IDLE_PER_ORIGIN:
                rows.append((connection, time.monotonic()))
                return
        connection.close()

    def _exchange(
        self,
        origin: _Origin,
        connection: http.client.HTTPConnection,
        path: str,
        body: bytes | None,
        *,
        reused: bool,
    ) -> bytes:
        try:
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
            except ConnectionError:
                if reused:
                    raise _Stale from None
                raise
            if response.status != 200:
                fields = response.headers.get_all("Retry-After") or []
                raise TransportError(
                    f"HTTP endpoint returned status {response.status}",
                    transient=response.status in TRANSIENT_STATUSES,
                    retry_after=_retry_after(fields[0], time.time()) if len(fields) == 1 else None,
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
        except BaseException as error:
            connection.close()
            if isinstance(error, (_Stale, TransportError)):
                raise
            if isinstance(error, TimeoutError):
                # A slow backend or a stalled connection; a new attempt may not be.
                raise TransportError("HTTP request failed", transient=True) from None
            if isinstance(error, (ssl.SSLCertVerificationError, ValueError)):
                # Do not echo endpoint credentials, remote bodies or TLS file paths.
                raise TransportError("HTTP request failed") from None
            if isinstance(error, (OSError, http.client.HTTPException)):
                # Refused, reset or dropped connections.
                raise TransportError("HTTP request failed", transient=True) from None
            raise
        # Reuse only a fully read response that does not end its connection.
        if response.isclosed() and not response.will_close:
            self._keep(origin, connection)
        else:
            connection.close()
        return raw
