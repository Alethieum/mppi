"""Coherent canonical state caches, with bounded observation and head age.

All RPC work runs off the owning asyncio loop. Admission reads only this cache.
No provider storage, event index, transaction retry or execution control is added.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from .chain import Block, ChainEvidenceError, ChainReader, ChainSession
from .discovery import ProviderIdentity
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
        self._running = False

    @property
    def current(self) -> ChainSnapshot | None:
        """Last observation, including stale evidence. Never an admission grant."""
        return self._current

    async def start(self) -> None:
        if self._running:
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
        block = self.reader.block()
        if previous is not None:
            old = previous.block
            if self.reader.block(old.observation.block_number) != old:
                raise ChainEvidenceError("previously accepted canonical block changed")
            if (
                block.observation.block_number == old.observation.block_number + 1
                and block.parent_hash != old.observation.block_hash
            ):
                raise ChainEvidenceError("canonical head ancestry mismatch")
        self.reader.verify_deployment(block)
        pause = self.reader.pause_level(block)
        identities = {
            pid: self.reader.identity(pid, block, allow_unavailable_catalog=True)
            for pid in providers
        }
        states = {sid: self.reader.session(sid, block) for sid in sessions}
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
                    raise ChainEvidenceError("confirmed session evidence regressed")
        if self.reader.block(block.observation.block_number) != block:
            raise ChainEvidenceError("canonical cache position changed during read")
        return ChainSnapshot(
            block, pause, MappingProxyType(identities), MappingProxyType(states), self._clock()
        )

    async def refresh(self) -> ChainSnapshot:
        async with self._lock:
            try:
                result = await asyncio.to_thread(
                    self._read, tuple(self._providers), tuple(self._sessions), self._current
                )
                previous = self._current
                if previous is not None:
                    old = previous.block
                    new = result.block
                    if (
                        new.observation.block_number < old.observation.block_number
                        or new.timestamp < old.timestamp
                        or (
                            new.observation.block_number == old.observation.block_number
                            and new != old
                        )
                    ):
                        raise ChainEvidenceError("canonical observation regressed or changed")
                if (
                    previous is None
                    or result.block.observation.block_number
                    > previous.block.observation.block_number
                ):
                    self._progress_at = result.observed_at
                self._current = result
                self._failed = False
                return self.snapshot()
            except BaseException:
                self._failed = True
                raise

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
        if session_id is not None:
            fixed_bytes(session_id, 32, field="session_id")

        def failure(
            reason: str,
            *,
            code: PaymentCode = PaymentCode.SESSION_NOT_READY,
            pause: PauseLevel | None = None,
        ) -> Readiness:
            return Readiness(
                False, (PaymentFailure(code, reason, session_id=session_id, pause_level=pause),)
            )

        try:
            current = self.snapshot()
        except ChainEvidenceError:
            return failure("CHAIN_STATE_STALE")
        if current.pause_level is not PauseLevel.NONE:
            return failure("CONTRACT_PAUSED", pause=current.pause_level)
        if session_id is None:
            return Readiness(True)
        state = current.sessions.get(session_id)
        if state is None:
            return failure("STATE_UNAVAILABLE")
        if not state.exists:
            return failure("SESSION_NOT_FOUND", code=PaymentCode.SESSION_NOT_FOUND)
        if state.status is SessionStatus.CLOSED:
            return failure("SESSION_CLOSED")
        if state.status is SessionStatus.CLOSING:
            return failure("CLOSE_PENDING")
        if (
            self.estimated_timestamp() + self.collection_margin_ms / 1000
            >= state.last_activity + 86400
        ):
            return failure("CLAIM_DEADLINE")
        return Readiness(True)
