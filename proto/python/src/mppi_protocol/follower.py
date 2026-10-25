"""Coherent canonical state caches, with bounded observation and head age.

All RPC work runs off the owning asyncio loop. Admission reads only this cache.
No provider storage, event index, transaction retry or execution control is added.
A provider admits only on fresh evidence (``readiness``); an agent's requests
within an attached session use the last accepted evidence whatever its age
(``confirmed_readiness``), and money steps refresh first.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from .chain import (
    Block,
    ChainContradictionError,
    ChainEvidenceError,
    ChainLagError,
    ChainReader,
    ChainSession,
)
from .discovery import ProviderIdentity
from .errors import ValidationError
from .http import TransportError
from .types import (
    PauseLevel,
    PaymentCode,
    PaymentFailure,
    Readiness,
    SessionStatus,
    fixed_bytes,
    uint256,
)


def _milliseconds(value: int, field: str) -> int:
    if type(value) is not int or not 0 < value <= ((1 << 63) - 1) // 1_000_000:
        raise ValueError(f"{field} must be positive bounded integer milliseconds")
    return value


# A failed refresh is repeated once, 250 ms later, before it fails closed.
_REFRESH_RETRY_DELAYS = (0.25,)
_sleep = asyncio.sleep


@dataclass(frozen=True, slots=True)
class RefreshFailure:
    """The last failed refresh attempt: error class, SDK reason, monotonic time.

    The reason is SDK-written text only, never an endpoint or remote content.
    """

    kind: str
    reason: str
    at: float


@dataclass(frozen=True, slots=True)
class ChainSnapshot:
    block: Block
    pause_level: PauseLevel
    identities: Mapping[int, ProviderIdentity]
    sessions: Mapping[bytes, ChainSession]
    observed_at: float


class ChainFollower:
    """One loop owns this follower. The caller supplies its trusted reader/profile.

    Freshness is bounded by monotonic observation age, head progress and block
    timestamp age. A responding RPC repeating its head cannot renew freshness.
    Local time must be synchronized to the selected chain; skew fails closed.
    """

    def __init__(
        self,
        reader: ChainReader,
        *,
        max_age_ms: int,
        collection_margin_ms: int,
        poll_interval_ms: int | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self.reader = reader
        self.max_age_ms = _milliseconds(max_age_ms, "max_age_ms")
        self.collection_margin_ms = _milliseconds(collection_margin_ms, "collection_margin_ms")
        self.poll_interval_ms = _milliseconds(
            max(1, max_age_ms // 4) if poll_interval_ms is None else poll_interval_ms,
            "poll_interval_ms",
        )
        if self.poll_interval_ms > self.max_age_ms:
            raise ValueError("poll interval exceeds freshness limit")
        self._clock = monotonic
        self._wall = wall_clock
        self._providers: set[int] = set()
        self._sessions: set[bytes] = set()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._current: ChainSnapshot | None = None
        self._progress_at = 0.0
        self._failed = True
        # Set by a refresh that found contradicting evidence, cleared by the next
        # accepted one: confirmed state is no longer evidence for requests.
        self._contradicted = False
        self._running = False
        self.last_refresh_failure: RefreshFailure | None = None
        # Called, outside the refresh lock, after each failed background refresh.
        self.on_refresh_failure: Callable[[RefreshFailure], None] | None = None

    @property
    def current(self) -> ChainSnapshot | None:
        """Last observation, including stale evidence. Never an admission grant."""
        return self._current

    def follow(self, *, providers: tuple[int, ...] = (), sessions: tuple[bytes, ...] = ()) -> None:
        """Watch these from the next refresh on, without reading now."""
        for provider_id in providers:
            self._providers.add(uint256(provider_id, field="provider_id"))
        for session_id in sessions:
            self._sessions.add(fixed_bytes(session_id, 32, field="session_id"))

    async def start(self) -> None:
        """Begin following. One refresh covers everything watched so far; if
        already following, refresh only when the snapshot does not cover it."""
        if self._running:
            if not self._covers_watched():
                await self.refresh()
            return
        self._running = True
        try:
            await self.refresh()
        finally:
            # Keep retrying reads after unavailable startup; readiness stays false.
            self._task = asyncio.create_task(self._poll(), name="mppi-chain-follower")

    async def stop(self) -> None:
        self._running = False
        self._failed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _poll(self) -> None:
        while self._running:
            await asyncio.sleep(self.poll_interval_ms / 1000)
            try:
                await self.refresh()
            except Exception:
                # Keep last evidence for accounting, never silently extend it.
                self._failed = True
                callback, failure = self.on_refresh_failure, self.last_refresh_failure
                if callback is not None and failure is not None:
                    try:
                        callback(failure)
                    except Exception:
                        pass  # An observer never stops following.

    async def watch_provider(self, provider_id: int) -> ProviderIdentity:
        uint256(provider_id, field="provider_id")
        self._providers.add(provider_id)
        return (await self.refresh()).identities[provider_id]

    async def watch_session(self, session_id: bytes) -> ChainSession:
        fixed_bytes(session_id, 32, field="session_id")
        self._sessions.add(session_id)
        return (await self.refresh()).sessions[session_id]

    def _read(
        self,
        providers: tuple[int, ...],
        sessions: tuple[bytes, ...],
        previous: ChainSnapshot | None,
    ) -> ChainSnapshot:
        if self._final():
            # One round trip: a committed block is final, so none is re-read.
            block, pause, identities, states = self.reader.latest(
                providers=providers, sessions=sessions
            )
            old = previous.block if previous is not None else None
            if old is not None and block.observation.block_number < old.observation.block_number:
                raise ChainLagError("RPC head is behind accepted chain evidence")
            return self._checked(block, pause, identities, states, previous)
        # Two round trips: the head, then everything read at it.
        (head,) = self.reader._blocks((None,))
        return self._snapshot_at(
            head.observation.block_number,
            head.observation.block_hash,
            providers,
            sessions,
            previous,
        )

    def _final(self) -> bool:
        profile = getattr(self.reader, "profile", None)
        return bool(getattr(profile, "instant_finality", False)) and hasattr(self.reader, "latest")

    def _snapshot_at(
        self,
        number: int,
        block_hash: bytes,
        providers: tuple[int, ...],
        sessions: tuple[bytes, ...],
        previous: ChainSnapshot | None,
    ) -> ChainSnapshot:
        """Everything watched, read at one block in one batch, which also
        re-reads the previously accepted block by number."""
        old = previous.block if previous is not None else None
        if old is not None and number < old.observation.block_number:
            raise ChainLagError("RPC head is behind accepted chain evidence")
        final = self._final()
        block, again, pause, identities, states = self.reader._observe_at(
            number,
            block_hash,
            providers=providers,
            sessions=sessions,
            recheck=old.observation.block_number if old is not None and not final else None,
        )
        if old is not None and not final:
            if again != old:
                raise ChainContradictionError("previously accepted canonical block changed")
            if (
                block.observation.block_number == old.observation.block_number + 1
                and block.parent_hash != old.observation.block_hash
            ):
                raise ChainContradictionError("canonical head ancestry mismatch")
        return self._checked(block, pause, identities, states, previous)

    def _checked(
        self,
        block: Block,
        pause: PauseLevel,
        identities: Mapping[int, ProviderIdentity],
        states: Mapping[bytes, ChainSession],
        previous: ChainSnapshot | None,
    ) -> ChainSnapshot:
        """A new snapshot, unless a confirmed session's state went backwards."""
        if previous is not None:
            order = {SessionStatus.OPEN: 0, SessionStatus.CLOSING: 1, SessionStatus.CLOSED: 2}
            for sid, state in states.items():
                prior = previous.sessions.get(sid)
                if (
                    prior is not None
                    and prior.exists
                    and (
                        state.deposit < prior.deposit
                        or state.settled_cumulative < prior.settled_cumulative
                        or order[state.status] < order[prior.status]
                        or state.last_activity < prior.last_activity
                        or state.initial_rate_card_hash != prior.initial_rate_card_hash
                        or (
                            prior.close_requested_at != 0
                            and state.close_requested_at != prior.close_requested_at
                        )
                    )
                ):
                    raise ChainContradictionError("confirmed session evidence regressed")
        return ChainSnapshot(
            block, pause, MappingProxyType(identities), MappingProxyType(states), self._clock()
        )

    async def refresh_at(self, number: int, block_hash: bytes) -> ChainSnapshot:
        """Refresh at a block another party named, such as a provider's snapshot,
        in one round trip. A block that is not canonical at that number fails
        this call only: the follower keeps its accepted evidence."""
        async with self._lock:
            self._accept(
                await asyncio.to_thread(
                    self._snapshot_at,
                    number,
                    block_hash,
                    tuple(self._providers),
                    tuple(self._sessions),
                    self._current,
                )
            )
            return self.snapshot()

    async def refresh(self) -> ChainSnapshot:
        """Read a new canonical position; repeat a failed read once.

        A head or block behind accepted evidence is lag (ChainLagError): if it
        persists, the accepted snapshot is kept and ages as usual, so freshness
        is never extended. Any other failure fails closed until a later read.
        """
        async with self._lock:
            try:
                for delay in (*_REFRESH_RETRY_DELAYS, None):
                    try:
                        self._accept(
                            await asyncio.to_thread(
                                self._read,
                                tuple(self._providers),
                                tuple(self._sessions),
                                self._current,
                            )
                        )
                        break
                    except Exception as error:
                        if isinstance(error, ChainContradictionError):
                            self._contradicted = True
                        self.last_refresh_failure = RefreshFailure(
                            type(error).__name__,
                            str(error)[:200]
                            if isinstance(
                                error, (ChainEvidenceError, TransportError, ValidationError)
                            )
                            else "",
                            self._clock(),
                        )
                        if delay is None:
                            if isinstance(error, ChainLagError) and self._covers_watched():
                                break
                            raise
                    await _sleep(delay)
                return self.snapshot()
            except BaseException:
                self._failed = True
                raise

    def _accept(self, result: ChainSnapshot) -> None:
        previous = self._current
        if previous is not None:
            old = previous.block
            new = result.block
            if (
                new.observation.block_number < old.observation.block_number
                or new.timestamp < old.timestamp
                or (new.observation.block_number == old.observation.block_number and new != old)
            ):
                raise ChainContradictionError("canonical observation regressed or changed")
        if (
            previous is None
            or result.block.observation.block_number > previous.block.observation.block_number
        ):
            self._progress_at = result.observed_at
        self._current = result
        self._failed = False
        self._contradicted = False

    def confirm(self, session_id: bytes, state: ChainSession) -> None:
        """Record a session state proved from a receipt since the last refresh.

        The agent's own opening, top-up and close are proved by their events,
        so no refresh follows them; the next refresh replaces this. It never
        renews freshness, and a state behind the accepted one is refused.
        """
        fixed_bytes(session_id, 32, field="session_id")
        current = self._current
        if current is None:
            raise ChainEvidenceError("no accepted observation to extend")
        self._sessions.add(session_id)
        prior = current.sessions.get(session_id)
        if (
            prior is not None
            and prior.exists
            and (
                state.deposit < prior.deposit or state.settled_cumulative < prior.settled_cumulative
            )
        ):
            raise ChainContradictionError("confirmed session evidence regressed")
        self._current = replace(
            current, sessions=MappingProxyType({**current.sessions, session_id: state})
        )

    def _covers_watched(self) -> bool:
        current = self._current
        return (
            current is not None
            and not self._failed
            and all(provider in current.identities for provider in self._providers)
            and all(session in current.sessions for session in self._sessions)
        )

    def snapshot(self) -> ChainSnapshot:
        result = self._current
        now = self._clock()
        limit = self.max_age_ms / 1000
        if (
            not self._running
            or self._failed
            or result is None
            or now < result.observed_at
            or now - result.observed_at >= limit
            or now < self._progress_at
            or now - self._progress_at >= limit
            or self._wall() - result.block.timestamp >= limit
            or result.block.timestamp > self._wall()
        ):
            raise ChainEvidenceError("chain observation is unavailable or stale")
        return result

    def estimated_timestamp(self) -> float:
        current = self.snapshot()
        return max(self._wall(), current.block.timestamp + self._clock() - current.observed_at)

    def readiness(self, session_id: bytes | None = None) -> Readiness:
        """Fresh evidence only: what a provider admits on, and money steps need."""
        if session_id is not None:
            fixed_bytes(session_id, 32, field="session_id")
        try:
            current = self.snapshot()
        except ChainEvidenceError:
            return _failure("CHAIN_STATE_STALE", session_id)
        return self._readiness(current, session_id, self.estimated_timestamp())

    def confirmed_readiness(self, session_id: bytes) -> Readiness:
        """The same rules on the last accepted evidence, whatever its age.

        For an agent's requests within an attached session, which need no chain
        read: the contract lets its provider collect only vouchers the session
        signer signed, up to the deposit. Deposits and activity only grow, so
        older evidence only understates the deposit and the collection
        deadline, and errs toward refusal. Never a provider admission grant.

        A refresh that found evidence contradicting the deployment or what was
        accepted (ChainContradictionError) holds it at INCONSISTENT_STATE until
        a later refresh is accepted; unavailable or lagging reads never do.
        """
        fixed_bytes(session_id, 32, field="session_id")
        current = self._current
        if current is None:
            return _failure("STATE_UNAVAILABLE", session_id)
        if self._contradicted:
            return _failure("INCONSISTENT_STATE", session_id)
        return self._readiness(current, session_id, max(self._wall(), current.block.timestamp))

    def _readiness(self, current: ChainSnapshot, session_id: bytes | None, now: float) -> Readiness:
        if current.pause_level is not PauseLevel.NONE:
            return _failure("CONTRACT_PAUSED", session_id, pause=current.pause_level)
        if session_id is None:
            return Readiness(True)
        state = current.sessions.get(session_id)
        if state is None:
            return _failure("STATE_UNAVAILABLE", session_id)
        if not state.exists:
            return _failure("SESSION_NOT_FOUND", session_id, code=PaymentCode.SESSION_NOT_FOUND)
        if state.status is SessionStatus.CLOSED:
            return _failure("SESSION_CLOSED", session_id)
        if state.status is SessionStatus.CLOSING:
            return _failure("CLOSE_PENDING", session_id)
        if now + self.collection_margin_ms / 1000 >= state.last_activity + 86400:
            return _failure("CLAIM_DEADLINE", session_id)
        return Readiness(True)


def _failure(
    reason: str,
    session_id: bytes | None,
    *,
    code: PaymentCode = PaymentCode.SESSION_NOT_READY,
    pause: PauseLevel | None = None,
) -> Readiness:
    return Readiness(
        False, (PaymentFailure(code, reason, session_id=session_id, pause_level=pause),)
    )
