"""Freshness must stop grants without a per-token RPC call."""

import asyncio
from dataclasses import replace

import pytest
from mppi_protocol import follower as follower_module
from mppi_protocol.chain import (
    Block,
    ChainContradictionError,
    ChainEvidenceError,
    ChainLagError,
    ChainSession,
)
from mppi_protocol.discovery import ProviderIdentity
from mppi_protocol.follower import ChainFollower
from mppi_protocol.http import TransportError
from mppi_protocol.types import ChainObservation, PauseLevel, SessionStatus

SID = b"s" * 32


class Clock:
    mono = 10.0
    wall = 1800000000.0

    def advance(self, seconds):
        self.mono += seconds
        self.wall += seconds


class Reader:
    def __init__(self, clock):
        self.clock = clock
        self.head = Block(
            ChainObservation(5042002, b"p" * 20, 10, b"b" * 32), int(clock.wall), b"a" * 32
        )
        self.state = ChainSession(100, 0, SessionStatus.OPEN, 0, int(clock.wall), b"h" * 32)
        self.initial_head = self.head
        self.pause = PauseLevel.NONE
        self.fail = False
        self.calls = 0

    def block(self, number=None):
        self.calls += 1
        if isinstance(self.fail, Exception):
            raise self.fail
        if self.fail:
            raise ChainEvidenceError("unavailable")
        if number == 10 and self.head.observation.block_number > 10:
            return self.initial_head
        return self.head

    def verify_deployment(self, block):
        assert block == self.head

    def pause_level(self, block):
        assert block == self.head
        return self.pause

    def session(self, sid, block):
        assert sid == SID and block == self.head
        return self.state

    # ChainReader's batched forms, here built from the single reads above.
    def _blocks(self, numbers):
        return [self.block(number) for number in numbers]

    def _observe_at(self, number, block_hash, *, providers=(), sessions=(), recheck=None):
        block = self.block(number)
        again = self.block(recheck) if recheck is not None else None
        if block.observation.block_hash != block_hash:
            raise ChainEvidenceError("canonical block evidence changed during observation")
        self.verify_deployment(block)
        pause = self.pause_level(block)
        identities = {
            provider: self.identity(provider, block, allow_unavailable_catalog=True)
            for provider in providers
        }
        states = {sid: self.session(sid, block) for sid in sessions}
        return block, again, pause, identities, states


def setup():
    clock = Clock()
    reader = Reader(clock)
    follower = ChainFollower(
        reader,
        max_age_ms=10000,
        collection_margin_ms=1000,
        monotonic=lambda: clock.mono,
        wall_clock=lambda: clock.wall,
    )
    return clock, reader, follower


def test_cached_checks_and_repeated_head_cannot_renew_freshness():
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        calls = reader.calls
        for _ in range(100):
            assert follower.readiness(SID).ready
        assert reader.calls == calls
        clock.advance(9)
        await follower.refresh()
        clock.advance(1)
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert follower.readiness(SID).failures[0].reason == "CHAIN_STATE_STALE"
        await follower.stop()

    asyncio.run(run())


def test_new_head_recovers_but_failed_read_immediately_fails_closed():
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        clock.advance(11)
        assert not follower.readiness(SID).ready
        reader.head = replace(
            reader.head,
            observation=replace(reader.head.observation, block_number=11, block_hash=b"c" * 32),
            timestamp=int(clock.wall),
            parent_hash=reader.head.observation.block_hash,
        )
        await follower.refresh()
        assert follower.readiness(SID).ready
        reader.fail = True
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert not follower.readiness(SID).ready
        assert follower.current.sessions[SID].deposit == 100
        await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"deposit": 0}, "SESSION_NOT_FOUND"),
        ({"status": SessionStatus.CLOSED}, "SESSION_CLOSED"),
        ({"status": SessionStatus.CLOSING}, "CLOSE_PENDING"),
        ({"last_activity": 1800000000 - 86400 + 1}, "CLAIM_DEADLINE"),
    ],
)
def test_lifecycle_and_strict_deadline_margin(change, reason):
    async def run():
        _, reader, follower = setup()
        reader.state = replace(reader.state, **change)
        await follower.start()
        await follower.watch_session(SID)
        assert follower.readiness(SID).failures[0].reason == reason
        await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize("pause", [PauseLevel.PAYMENTS, PauseLevel.ALL])
def test_active_pause_blocks_grants_with_observed_level(pause):
    async def run():
        _, reader, follower = setup()
        reader.pause = pause
        await follower.start()
        await follower.watch_session(SID)
        failure = follower.readiness(SID).failures[0]
        assert failure.reason == "CONTRACT_PAUSED" and failure.pause_level == pause
        await follower.stop()

    asyncio.run(run())


def test_reorg_or_regression_does_not_replace_accepted_cache():
    async def run():
        _, reader, follower = setup()
        await follower.start()
        old = follower.current
        reader.head = replace(
            reader.head, observation=replace(reader.head.observation, block_hash=b"x" * 32)
        )
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert follower.current is old and not follower.readiness().ready
        await follower.stop()

    asyncio.run(run())


def test_old_or_far_future_head_is_not_fresh_on_initial_contact():
    async def run():
        for skew in (-20, 1):
            _, reader, follower = setup()
            reader.head = replace(reader.head, timestamp=reader.head.timestamp + skew)
            with pytest.raises(ChainEvidenceError):
                await follower.start()
            assert not follower.readiness().ready
            await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize(
    "change",
    [
        {"deposit": 99},
        {"settled_cumulative": 1},
        {"initial_rate_card_hash": b"x" * 32},
        {"last_activity": 1},
    ],
)
def test_confirmed_session_state_cannot_regress(change):
    async def run():
        _, reader, follower = setup()
        reader.state = replace(reader.state, settled_cumulative=2)
        await follower.start()
        await follower.watch_session(SID)
        old = follower.current
        reader.state = replace(reader.state, **change)
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert follower.current is old
        await follower.stop()

    asyncio.run(run())


def test_new_head_requires_previously_accepted_ancestry():
    async def run():
        _, reader, follower = setup()
        await follower.start()
        reader.head = replace(
            reader.head,
            observation=replace(reader.head.observation, block_number=11, block_hash=b"x" * 32),
            parent_hash=b"y" * 32,
        )
        with pytest.raises(ChainEvidenceError, match="ancestry"):
            await follower.refresh()
        await follower.stop()

    asyncio.run(run())


def test_absent_offering_preserves_fresh_session_and_pause_observations():
    async def run():
        _, reader, follower = setup()

        def identity(provider, block, *, allow_unavailable_catalog=False):
            assert allow_unavailable_catalog is True
            return ProviderIdentity(
                block.observation,
                b"r" * 20,
                provider,
                b"o" * 20,
                b"t" * 20,
                "https://provider.example/discover",
                None,
            )

        reader.identity = identity
        await follower.start()
        current = await follower.watch_provider(42)
        assert current.catalog is None
        await follower.watch_session(SID)
        assert follower.snapshot().sessions[SID].exists
        reader.pause = PauseLevel.PAYMENTS
        await follower.refresh()
        assert follower.readiness(SID).failures[0].reason == "CONTRACT_PAUSED"
        assert follower.snapshot().identities[42].catalog is None
        await follower.stop()

    asyncio.run(run())


@pytest.fixture
def no_wait(monkeypatch):
    async def sleep(_):
        return None

    monkeypatch.setattr(follower_module, "_sleep", sleep)


def advance_head(reader, clock, number):
    previous = reader.head
    reader.head = replace(
        previous,
        observation=replace(
            previous.observation, block_number=number, block_hash=bytes([number]) * 32
        ),
        timestamp=int(clock.wall),
        parent_hash=previous.observation.block_hash,
    )
    return previous


def test_a_failed_refresh_is_repeated_once_and_its_cause_is_kept(no_wait):
    """One failed read no longer refuses every request until the next poll."""

    async def run():
        _, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        failures = iter([True])
        original = reader.block

        def flaky(number=None):
            if next(failures, False):
                raise ChainEvidenceError("unavailable")
            return original(number)

        reader.block = flaky
        await follower.refresh()
        assert follower.readiness(SID).ready
        failure = follower.last_refresh_failure
        assert (failure.kind, failure.reason) == ("ChainEvidenceError", "unavailable")
        await follower.stop()

    asyncio.run(run())


def test_a_lagging_rpc_head_keeps_accepted_evidence_without_failing(no_wait):
    """A load-balanced RPC may serve a lower head; that is lag, not regression."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        clock.advance(1)
        lagging = advance_head(reader, clock, 11)
        await follower.refresh()
        accepted = follower.current
        reader.head = lagging  # The next backend is one block behind.
        assert (await follower.refresh()) is accepted
        assert follower.readiness(SID).ready and follower.current is accepted
        assert follower.last_refresh_failure.kind == "ChainLagError"
        # Lag never extends freshness: the accepted evidence still ages out.
        clock.advance(10)
        with pytest.raises(ChainEvidenceError, match="stale"):
            await follower.refresh()
        assert not follower.readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


def test_an_accepted_block_not_yet_served_is_lag(no_wait):
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        clock.advance(1)
        advance_head(reader, clock, 11)
        await follower.refresh()
        accepted = follower.current
        original = reader.block

        def behind(number=None):
            if number == 11:
                raise ChainLagError("RPC has not served that block yet")
            return original(number)

        reader.block = behind
        assert (await follower.refresh()) is accepted
        # A new watch cannot be satisfied from older evidence.
        with pytest.raises(ChainLagError):
            await follower.watch_session(SID)
        await follower.stop()

    asyncio.run(run())


def test_persistent_failures_still_fail_closed_with_only_sdk_text(no_wait):
    async def run():
        _, reader, follower = setup()
        await follower.start()
        reader.fail = True
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert not follower.readiness().ready
        assert follower.last_refresh_failure.reason == "unavailable"
        await follower.stop()

    asyncio.run(run())


def test_confirmed_readiness_keeps_the_last_evidence_while_the_rpc_is_down(no_wait):
    """An agent's requests need no chain read; only fresh admission does."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        reader.fail = True
        clock.advance(120)
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()
        assert follower.readiness(SID).failures[0].reason == "CHAIN_STATE_STALE"
        assert follower.confirmed_readiness(SID).ready
        assert follower.confirmed_readiness(b"t" * 32).failures[0].reason == "STATE_UNAVAILABLE"
        # The same local rules still apply, from the last confirmed activity.
        clock.advance(86400)
        assert follower.confirmed_readiness(SID).failures[0].reason == "CLAIM_DEADLINE"
        await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize(
    "contradiction",
    [
        "proxy implementation differs from release profile",
        "deployed code differs from release profile",
        "MPPI signing domain differs from release profile",
        "RPC chain differs from the selected deployment",
        "regressed",
    ],
)
def test_contradicting_evidence_stops_requests_until_a_refresh_is_accepted(no_wait, contradiction):
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        good = reader.state
        if contradiction == "regressed":
            reader.state = replace(good, deposit=good.deposit - 1)
        else:
            reader.fail = ChainContradictionError(contradiction)
        clock.advance(1)
        with pytest.raises(ChainContradictionError):
            await follower.refresh()
        assert follower.confirmed_readiness(SID).failures[0].reason == "INCONSISTENT_STATE"
        reader.fail, reader.state = False, good
        await follower.refresh()
        assert follower.confirmed_readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize(
    "unavailable",
    [
        TransportError("HTTP request failed", transient=True),
        TransportError("HTTP endpoint returned status 429", transient=True),
        TimeoutError(),
        ChainLagError("RPC head is behind accepted chain evidence"),
        ChainEvidenceError("RPC snapshot and head are from different blocks"),
    ],
)
def test_unavailable_or_lagging_reads_never_stop_requests(no_wait, unavailable):
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        reader.fail = unavailable
        clock.advance(1)
        try:
            await follower.refresh()
        except (ChainEvidenceError, TransportError, TimeoutError):
            pass  # Lag that the accepted snapshot covers is not raised.
        assert follower.confirmed_readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


def test_a_failed_background_refresh_is_reported_without_extending_freshness(no_wait):
    async def run():
        clock = Clock()
        reader = Reader(clock)
        follower = ChainFollower(
            reader,
            max_age_ms=10000,
            collection_margin_ms=1000,
            poll_interval_ms=1,
            monotonic=lambda: clock.mono,
            wall_clock=lambda: clock.wall,
        )
        seen = []

        def observe(failure):
            seen.append(failure)
            raise RuntimeError("an observer's own failure")  # Never stops following.

        follower.on_refresh_failure = observe
        await follower.start()
        reader.fail = True
        async with asyncio.timeout(2):
            while len(seen) < 2:
                await asyncio.sleep(0.01)
        assert (seen[0].kind, seen[0].reason) == ("ChainEvidenceError", "unavailable")
        assert not follower.readiness().ready
        await follower.stop()

    asyncio.run(run())


def test_a_proven_state_extends_the_view_without_renewing_freshness():
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        observed = follower.current.observed_at
        topped = replace(reader.state, deposit=150)
        follower.confirm(SID, topped)
        assert follower.current.sessions[SID] == topped
        assert follower.current.observed_at == observed  # Never fresher.
        new = b"n" * 32
        follower.confirm(new, replace(reader.state, deposit=10))
        assert new in follower.current.sessions
        with pytest.raises(ChainEvidenceError, match="regressed"):
            follower.confirm(SID, replace(topped, deposit=149))
        clock.advance(11)
        assert not follower.readiness(SID).ready
        assert follower.confirmed_readiness(SID).ready
        await follower.stop()

    asyncio.run(run())
