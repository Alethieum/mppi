"""Freshness must stop grants without a per-token RPC call."""

import asyncio
import threading
from dataclasses import replace

import pytest
from mppi_protocol import follower as follower_module
from mppi_protocol.chain import (
    Block,
    ChainContradictionError,
    ChainEvidenceError,
    ChainLagError,
    ChainSession,
    FundingState,
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
        self.others = {}  # Other sessions' states; absent ones hold no deposit.
        self.bound, self.code = b"w" * 20, False  # The watched funding wallet's.
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
        assert block == self.head
        if sid != SID:
            return self.others.get(sid, ChainSession(0, 0, SessionStatus.OPEN, 0, 0, bytes(32)))
        return self.state

    # ChainReader's batched forms, here built from the single reads above.
    def _blocks(self, numbers):
        return [self.block(number) for number in numbers]

    def _observe_at(
        self, number, block_hash, *, providers=(), sessions=(), recheck=None, funding=None
    ):
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
        return block, again, pause, identities, states, self.funding(funding)

    def funding(self, funding):
        if funding is None:
            return None
        agent_id, wallet = funding
        return FundingState(agent_id, wallet, self.bound, self.code)


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


PROVEN = ChainObservation(5042002, b"p" * 20, 20, b"t" * 32)


@pytest.mark.parametrize("operation", ["top_up", "close"])
def test_a_refresh_behind_a_proven_state_is_lag_not_a_regression(no_wait, operation):
    """A top-up or close proven at block 20 onto evidence at block 10. A
    load-balanced backend that has reached block 15 but not 20 still serves
    the earlier deposit or status: lag, and the proven state stays."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        if operation == "top_up":
            proven = replace(reader.state, deposit=150)
        else:
            proven = replace(reader.state, status=SessionStatus.CLOSED, settled_cumulative=40)
        follower.confirm(SID, proven, observation=PROVEN)
        readiness = follower.confirmed_readiness(SID)
        clock.advance(1)
        advance_head(reader, clock, 15)
        await follower.refresh()
        assert follower.last_refresh_failure.kind == "ChainLagError"
        assert follower.current.sessions[SID] == proven
        assert follower.confirmed_readiness(SID) == readiness
        # At or after the proving block the chain's state replaces it.
        reader.state = replace(proven, last_activity=proven.last_activity + 1)
        advance_head(reader, clock, 20)
        await follower.refresh()
        assert follower.current.sessions[SID] == reader.state
        await follower.stop()

    asyncio.run(run())


def test_an_unwatched_session_no_longer_holds_refreshes_behind_its_proof(no_wait):
    """A session proven at block 20 and then unwatched is not read again, so a
    refresh could never show it at or after block 20: its proof must go too."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        follower.confirm(SID, replace(reader.state, deposit=150), observation=PROVEN)
        follower.unwatch_session(SID)
        clock.advance(1)
        advance_head(reader, clock, 15)
        await follower.refresh()
        assert follower.last_refresh_failure is None
        assert follower.current.block.observation.block_number == 15
        await follower.stop()

    asyncio.run(run())


def test_a_proof_no_refresh_reaches_stops_its_state_after_max_age_without_latching(no_wait):
    """A proving block no backend reaches, as a wrong receipt block number
    would name, holds every refresh as lag. Once refreshes have ended behind it
    for longer than max_age_ms (10 s), the proven state is not ready for
    requests: STATE_UNAVAILABLE, never INCONSISTENT_STATE. A session with no
    proof keeps its older evidence, and an outage between lagging refreshes is
    unavailability, which alone never stops requests."""

    async def run():
        clock, reader, follower = setup()
        other = b"o" * 32
        reader.others[other] = replace(reader.state, deposit=50)
        await follower.start()
        await follower.watch_session(SID)
        await follower.watch_session(other)
        graft = replace(reader.state, deposit=10_000)
        follower.confirm(SID, graft, observation=replace(PROVEN, block_number=10**9))
        for number in range(11, 17):  # One a second: within 10 s of the first.
            clock.advance(1)
            advance_head(reader, clock, number)
            await follower.refresh()
            assert follower.last_refresh_failure.kind == "ChainLagError"
            assert follower.confirmed_readiness(SID).ready
        reader.fail = TransportError("HTTP request failed", transient=True)
        clock.advance(30)
        with pytest.raises(TransportError):
            await follower.refresh()
        assert follower.confirmed_readiness(SID).ready
        reader.fail = False
        clock.advance(1)
        advance_head(reader, clock, 17)
        with pytest.raises(ChainLagError):
            await follower.refresh()  # 36 s after the first refresh behind it.
        failure = follower.confirmed_readiness(SID).failures[0]
        assert (failure.reason, failure.session_id) == ("STATE_UNAVAILABLE", SID)
        assert follower.confirmed_readiness(other).ready
        assert follower.current.sessions[SID] == graft  # Kept, as evidence.
        await follower.stop()

    asyncio.run(run())


def test_an_rpc_that_reaches_a_proving_block_late_restores_its_state(no_wait):
    """An honest backend behind the proof for longer than max_age_ms stops the
    proven state; the first refresh accepted at its block restores it."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        proven = replace(reader.state, deposit=150)
        follower.confirm(SID, proven, observation=PROVEN)  # Block 20.
        for number in (11, 13, 15):  # Six seconds apart: 12 s behind it in all.
            clock.advance(6)
            advance_head(reader, clock, number)
            try:
                await follower.refresh()
            except ChainEvidenceError:
                pass  # Stale by now: lag never renews freshness.
            assert follower.last_refresh_failure.kind == "ChainLagError"
        assert follower.confirmed_readiness(SID).failures[0].reason == "STATE_UNAVAILABLE"
        reader.state = proven
        clock.advance(1)
        advance_head(reader, clock, 20)
        await follower.refresh()
        assert follower.current.sessions[SID] == proven
        assert follower.confirmed_readiness(SID).ready and follower.readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


async def lag_behind_the_proof_too_long(clock, reader, follower):
    """Refreshes end behind SID's proof at block 20 for 12 s, longer than
    max_age_ms (10 s), so its proven state is no longer ready."""
    follower.confirm(SID, replace(reader.state, deposit=150), observation=PROVEN)
    for number in (11, 13, 15):  # Six seconds apart.
        clock.advance(6)
        advance_head(reader, clock, number)
        try:
            await follower.refresh()
        except ChainEvidenceError:
            pass  # Stale by now: lag never renews freshness.
        assert follower.last_refresh_failure.kind == "ChainLagError"
    assert follower.confirmed_readiness(SID).failures[0].reason == "STATE_UNAVAILABLE"


def test_an_accepted_refresh_ends_the_lag_it_was_behind(no_wait):
    """The refresh that reaches the proving block clears the lag behind it. A
    later proof is ready at once, and its own lag counts from the first
    refresh that ends behind it, not from the earlier proof's."""

    async def run():
        clock, reader, follower = setup()
        served, plain = {}, reader.block

        def block(number=None):  # A block read again by number, as first served.
            found = plain(number)
            if number in served:
                return served[number]
            return served.setdefault(found.observation.block_number, found)

        reader.block = block
        await follower.start()
        await follower.watch_session(SID)
        await lag_behind_the_proof_too_long(clock, reader, follower)
        reader.state = replace(reader.state, deposit=150)
        clock.advance(1)
        advance_head(reader, clock, 20)
        await follower.refresh()
        assert follower.confirmed_readiness(SID).ready
        later = replace(PROVEN, block_number=30, block_hash=b"u" * 32)
        follower.confirm(SID, replace(reader.state, deposit=200), observation=later)
        assert follower.confirmed_readiness(SID).ready
        clock.advance(1)
        advance_head(reader, clock, 25)
        await follower.refresh()  # Behind block 30, 14 s after the earlier lag began.
        assert follower.last_refresh_failure.kind == "ChainLagError"
        assert follower.confirmed_readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


def test_unwatching_the_last_proven_session_ends_the_lag_behind_it(no_wait):
    """Its proof goes with the watch, and so does the lag behind it: another
    session proven before any refresh is accepted is ready at once, and its
    own lag counts from the first refresh that ends behind it."""

    async def run():
        clock, reader, follower = setup()
        other = b"o" * 32
        reader.others[other] = replace(reader.state, deposit=50)
        await follower.start()
        await follower.watch_session(SID)
        await follower.watch_session(other)
        await lag_behind_the_proof_too_long(clock, reader, follower)
        follower.unwatch_session(SID)
        later = replace(PROVEN, block_number=30, block_hash=b"u" * 32)
        follower.confirm(other, replace(reader.others[other], deposit=60), observation=later)
        assert follower.confirmed_readiness(other).ready
        clock.advance(1)
        advance_head(reader, clock, 16)
        with pytest.raises(ChainEvidenceError):
            await follower.refresh()  # Behind block 30, and the evidence is stale.
        assert follower.last_refresh_failure.kind == "ChainLagError"
        assert follower.confirmed_readiness(other).ready
        await follower.stop()

    asyncio.run(run())


@pytest.mark.parametrize("number", [20, 21])
def test_a_proven_state_regressed_at_or_after_its_block_is_a_contradiction(no_wait, number):
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        follower.confirm(SID, replace(reader.state, deposit=150), observation=PROVEN)
        reader.state = replace(reader.state, deposit=149)
        clock.advance(1)
        advance_head(reader, clock, number)
        with pytest.raises(ChainContradictionError, match="regressed"):
            await follower.refresh()
        assert follower.confirmed_readiness(SID).failures[0].reason == "INCONSISTENT_STATE"
        await follower.stop()

    asyncio.run(run())


def test_a_refresh_read_before_a_proven_session_was_watched_keeps_it(no_wait):
    """A new session proven at block 20 while a refresh that does not watch it
    yet is reading block 21: that read is behind the proof, and the repeat
    reads the session."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        new = b"n" * 32
        opened = ChainSession(10, 0, SessionStatus.OPEN, 0, int(clock.wall), b"h" * 32)
        reader.others[new] = opened
        entered, release = threading.Event(), threading.Event()
        original = reader._observe_at
        calls = []

        def held(number, block_hash, **watched):
            calls.append(watched["sessions"])
            entered.set()
            release.wait(5)
            return original(number, block_hash, **watched)

        reader._observe_at = held
        clock.advance(1)
        advance_head(reader, clock, 21)
        refresh = asyncio.create_task(follower.refresh())
        await asyncio.to_thread(entered.wait, 5)
        follower.confirm(new, opened, observation=PROVEN)
        release.set()
        await refresh
        assert calls == [(), (new,)]
        assert follower.current.sessions[new] == opened
        assert follower.current.block.observation.block_number == 21
        await follower.stop()

    asyncio.run(run())


def test_a_proof_at_the_accepted_block_keeps_its_state_from_a_read_begun_before_its_watch(
    no_wait,
):
    """An opening proven at the follower's own accepted block 20, while a
    refresh that does not watch it yet is reading block 21: that read is
    behind the proof too, so the repeat reads the session and the proven state
    is never dropped for evidence that never read it."""

    async def run():
        clock, reader, follower = setup()
        served, plain = {}, reader.block

        def block(number=None):  # A block read again by number, as first served.
            found = plain(number)
            if number in served:
                return served[number]
            return served.setdefault(found.observation.block_number, found)

        reader.block = block
        await follower.start()
        clock.advance(1)
        advance_head(reader, clock, 20)
        await follower.refresh()
        new = b"n" * 32
        opened = ChainSession(10, 0, SessionStatus.OPEN, 0, int(clock.wall), b"h" * 32)
        reader.others[new] = opened
        entered, release = threading.Event(), threading.Event()
        original = reader._observe_at
        calls = []

        def held(number, block_hash, **watched):
            calls.append(watched["sessions"])
            entered.set()
            release.wait(5)
            return original(number, block_hash, **watched)

        reader._observe_at = held
        clock.advance(1)
        advance_head(reader, clock, 21)
        refresh = asyncio.create_task(follower.refresh())
        await asyncio.to_thread(entered.wait, 5)
        accepted = follower.current.block.observation
        assert accepted.block_number == 20
        follower.follow(sessions=(new,))
        follower.confirm(new, opened, observation=accepted)
        release.set()
        await refresh
        assert calls == [(), (new,)]
        assert follower.current.sessions[new] == opened
        assert follower.current.block.observation.block_number == 21
        assert follower.confirmed_readiness(new).ready
        await follower.stop()

    asyncio.run(run())


def test_a_watched_funding_wallet_is_read_with_each_refresh_until_unwatched(no_wait):
    async def run():
        clock, reader, follower = setup()
        await follower.start()
        follower.follow(funding=(43, b"w" * 20))
        assert follower.current.funding is None
        calls = reader.calls
        await follower.start()  # Already following: it reads what is not covered yet.
        assert reader.calls > calls
        assert follower.current.funding == FundingState(43, b"w" * 20, b"w" * 20, False)
        calls = reader.calls
        await follower.start()
        assert reader.calls == calls
        follower.unwatch_funding()
        clock.advance(1)
        advance_head(reader, clock, 11)
        await follower.refresh()
        assert follower.current.funding is None
        await follower.stop()

    asyncio.run(run())


def test_a_proven_state_from_another_deployment_is_refused():
    async def run():
        _, reader, follower = setup()
        await follower.start()
        await follower.watch_session(SID)
        current = follower.current
        foreign = replace(PROVEN, contract_address=b"q" * 20)
        with pytest.raises(ChainEvidenceError, match="deployment"):
            follower.confirm(SID, replace(reader.state, deposit=150), observation=foreign)
        assert follower.current is current
        await follower.stop()

    asyncio.run(run())


def test_a_repeated_refresh_waits_a_jittered_share_of_its_delay(monkeypatch):
    """Followers refused at the same moment must not repeat in step."""
    waits = []

    async def sleep(delay):
        waits.append(delay)

    monkeypatch.setattr(follower_module, "_sleep", sleep)
    monkeypatch.setattr(follower_module, "_jitter", lambda: 0.5)

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
        assert waits == [0.125]
        await follower.stop()

    asyncio.run(run())


def test_watches_waiting_behind_a_refresh_share_the_next_one(no_wait):
    """Watches added while a refresh reads, as recoveries at startup add
    theirs, share the next refresh; lag still reaches each of them."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        heads, original = [], reader._blocks

        def counted(numbers):  # Each refresh's read begins with the head.
            heads.append(numbers)
            return original(numbers)

        reader._blocks = counted
        ids = [bytes([n]) * 32 for n in range(0x10, 0x30)]
        reader.others.update(dict.fromkeys(ids, reader.state))
        watched = await asyncio.gather(*(follower.watch_session(sid) for sid in ids))
        # The first watch's refresh began before the others were added; the
        # next one reads the 31 that waited behind it, for all of them.
        assert len(heads) == 2
        current = follower.snapshot()
        assert watched == [current.sessions[sid] for sid in ids]
        assert all(follower.readiness(sid).ready for sid in ids)
        # Lag reaches each waiting watch the accepted snapshot cannot cover,
        # from its own refresh and repeat, as before.
        clock.advance(1)
        lagging = advance_head(reader, clock, 11)
        await follower.refresh()
        reader.head = lagging
        new = [bytes([n]) * 32 for n in range(0x30, 0x38)]
        heads.clear()
        results = await asyncio.gather(
            *(follower.watch_session(sid) for sid in new), return_exceptions=True
        )
        assert all(isinstance(result, ChainLagError) for result in results)
        assert len(heads) == 2 * len(new)
        await follower.stop()

    asyncio.run(run())


def test_a_watch_behind_a_failed_refresh_is_not_answered_from_an_earlier_read(no_wait):
    """In the lock's order: a refresh reading, one that begins after the watch
    is added, one that fails after it, then the watch. The watch reads for
    itself and reports what its own refresh finds, as before, not the
    snapshot the failure left."""

    async def run():
        _, reader, follower = setup()
        await follower.start()
        entered, release = threading.Event(), threading.Event()
        heads, original = [], reader._blocks

        def scripted(numbers):
            heads.append(numbers)
            if len(heads) == 1:
                entered.set()
                release.wait(5)
            elif len(heads) > 2:
                raise ChainEvidenceError("unavailable")
            return original(numbers)

        reader._blocks = scripted
        first = asyncio.create_task(follower.refresh())
        await asyncio.to_thread(entered.wait, 5)
        shared = asyncio.create_task(follower.refresh())
        failing = asyncio.create_task(follower.refresh())
        watch = asyncio.create_task(follower.watch_session(SID))
        await asyncio.sleep(0)  # Each now waits for the lock, in that order.
        release.set()
        await first
        assert SID in (await shared).sessions
        with pytest.raises(ChainEvidenceError, match="unavailable"):
            await failing
        assert len(heads) == 4
        with pytest.raises(ChainEvidenceError, match="unavailable"):
            await watch
        assert len(heads) == 6
        assert not follower.readiness(SID).ready
        await follower.stop()

    asyncio.run(run())


def test_a_watch_reads_a_session_its_accepted_snapshot_holds_from_before_it(no_wait):
    """Watches share only a refresh begun after they were added. A session
    the accepted snapshot already holds without such a read, as one proven
    by confirm, is read by its watch, which returns the chain's state."""

    async def run():
        clock, reader, follower = setup()
        await follower.start()
        new = b"n" * 32
        proven = ChainSession(10, 0, SessionStatus.OPEN, 0, int(clock.wall), b"h" * 32)
        follower.confirm(new, proven)
        reader.others[new] = topped = replace(proven, deposit=15)
        heads, original = [], reader._blocks

        def counted(numbers):  # Each refresh's read begins with the head.
            heads.append(numbers)
            return original(numbers)

        reader._blocks = counted
        assert await follower.watch_session(new) == topped
        assert len(heads) == 1
        assert follower.current.sessions[new] == topped
        await follower.stop()

    asyncio.run(run())
