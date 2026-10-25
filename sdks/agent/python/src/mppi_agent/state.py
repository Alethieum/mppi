"""Agent-side validation of provider payment reports and recovery checkpoints."""

from __future__ import annotations

from dataclasses import dataclass

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol.amounts import TokenCounts, cumulative_usage, priced_numerator, required_coverage
from mppi_protocol.catalog import Catalog, parse_catalog
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.signing import session_id, verify_run_auth, verify_voucher
from mppi_protocol.types import Domain, RunContext, SessionDescriptor
from mppi_protocol.wire import descriptor_from_wire, validate_message


def invalid(message: str) -> ValidationError:
    return ValidationError(ValidationCode.INCONSISTENT_USAGE, message)


def counts(
    message: pb.Meter | pb.Terminal | pb.Receipt | pb.RequiredTokens | pb.PricedUsage | pb.RunState,
) -> TokenCounts:
    return TokenCounts(message.input_tokens, message.cache_tokens, message.output_tokens)


def monotone(previous: TokenCounts, current: TokenCounts, catalog: Catalog, model: str) -> None:
    if any(
        new < old
        for old, new in zip(
            (previous.input_tokens, previous.cache_tokens, previous.output_tokens),
            (current.input_tokens, current.cache_tokens, current.output_tokens),
            strict=True,
        )
    ) or priced_numerator(current, catalog.rates_for(model)) < priced_numerator(
        previous, catalog.rates_for(model)
    ):
        raise invalid("provider counts or exact cost regressed")


@dataclass(frozen=True, slots=True)
class Coverage:
    """One LowCredit's outcome for this agent.

    ``target`` is the cumulative voucher ceiling to sign now, if any.
    ``required`` is the USDC amount a coverage request requires, in six-decimal
    base units; above the deposit, or for a run that already ended, it signs
    nothing.
    ``superseded`` marks a request the provider computed before usage this
    agent has already applied; the provider's next check replaces it.
    ``numerator`` is a signable target's exact price. Deciding records
    nothing; Ledger.cover records it once the voucher is queued.
    """

    target: int | None = None
    required: int | None = None
    superseded: bool = False
    numerator: int | None = None


@dataclass(slots=True)
class Run:
    context: RunContext
    signature: bytes
    counts: TokenCounts = TokenCounts(0, 0, 0)
    meter_counts: TokenCounts = TokenCounts(0, 0, 0)
    meter_seq: int = 0
    final: tuple[int, int, int, int, int, int] | None = None
    failed_transport: bool = False
    # RunState proves final counts/status through a session cutover, but does
    # not carry the original run's final Meter sequence. Never invent one.
    recovered_through: int | None = None

    @property
    def active(self) -> bool:
        return self.final is None and not self.failed_transport


class Ledger:
    """Counts are trusted provider reports; signatures establish accepted prices."""

    def __init__(self, domain: Domain, descriptor: SessionDescriptor, initial: Catalog) -> None:
        self.domain, self.descriptor = domain, descriptor
        self.session_id = session_id(domain, descriptor)
        self.initial_hash = initial.rate_card_hash
        self.catalogs = {initial.rate_card_hash: initial}
        self.accepted_catalogs = {initial.rate_card_hash}
        self.runs: dict[bytes, Run] = {}
        self.usage: dict[tuple[str, bytes], TokenCounts] = {}
        self.meter_seq = 0
        self.generation = 0
        self.close_pending = False
        self.ready = False
        self.voucher: pb.Voucher | None = None
        self.coverage_numerator = 0

    @property
    def usage_numerator(self) -> int:
        return sum(
            priced_numerator(value, self.catalogs[version].rates_for(model))
            for (model, version), value in self.usage.items()
        )

    @property
    def usage_amount(self) -> int:
        return cumulative_usage((self.usage_numerator,))

    @property
    def pending(self) -> tuple[bytes, ...]:
        return tuple(identifier for identifier, run in self.runs.items() if run.final is None)

    def add_catalog(self, catalog: Catalog) -> None:
        previous = self.catalogs.get(catalog.rate_card_hash)
        if previous is not None and previous != catalog:
            raise invalid("catalog hash collision or conflicting evidence")
        self.catalogs[catalog.rate_card_hash] = catalog

    def register(self, context: RunContext, signature: bytes) -> Run:
        if context.run_id in self.runs or len(self.pending) >= 1024:
            raise invalid("duplicate run or pending recovery limit reached")
        if context.session_id != self.session_id or context.control_generation != self.generation:
            raise invalid("run differs from current attachment")
        self.catalogs[context.rate_card_hash].rates_for(context.model)
        verify_run_auth(self.domain, self.descriptor, context, signature)
        run = Run(context, signature)
        self.runs[context.run_id] = run
        return run

    def forget(self, identifier: bytes) -> None:
        """Drop a settled final run; its counts stay in the per-bucket usage.

        The caller decides that nothing can still change it (Client._prune).
        """
        if self._run(identifier).final is None:
            raise invalid("only a final run can be forgotten")
        del self.runs[identifier]

    def record_not_admitted(self, identifier: bytes) -> None:
        """Record a run the provider never admitted as NOT_ADMITTED.

        Its Complete was never sent, or the provider refused it before admission.
        It has no usage, no Meter cursor and no final sequence
        (``recovered_through``), and is never named pending again.
        """
        run = self._run(identifier)
        if run.final is not None or run.meter_seq or run.counts != TokenCounts(0, 0, 0):
            raise invalid("only an unreported pending run can be recorded not admitted")
        run.final = (0, 0, 0, pb.RUN_STATUS_NOT_ADMITTED, self.meter_seq, 0)
        run.recovered_through = self.meter_seq

    def _run(self, identifier: bytes) -> Run:
        try:
            return self.runs[identifier]
        except KeyError:
            raise invalid("report has no retained signed run") from None

    def _apply(self, run: Run, value: TokenCounts, deposit: int) -> None:
        key = (run.context.model, run.context.rate_card_hash)
        catalog = self.catalogs[key[1]]
        monotone(run.counts, value, catalog, key[0])
        old = self.usage.get(key, TokenCounts(0, 0, 0))
        updated = TokenCounts(
            old.input_tokens + value.input_tokens - run.counts.input_tokens,
            old.cache_tokens + value.cache_tokens - run.counts.cache_tokens,
            old.output_tokens + value.output_tokens - run.counts.output_tokens,
        )
        total = (
            self.usage_numerator
            - priced_numerator(old, catalog.rates_for(key[0]))
            + priced_numerator(updated, catalog.rates_for(key[0]))
        )
        if (
            self.voucher is None
            or total > self.voucher.cumulative * 1_000_000
            or total > deposit * 1_000_000
        ):
            raise invalid("reported usage exceeds signed authorization or deposit")
        cumulative_usage((total,))
        self.usage[key], run.counts = updated, value

    def accept_voucher(self, voucher: pb.Voucher, deposit: int) -> None:
        validate_message(voucher)
        if voucher.session_id != self.session_id or voucher.cumulative > deposit:
            raise invalid("voucher differs from session or deposit")
        verify_voucher(self.domain, self.descriptor, voucher.cumulative, voucher.signature)
        if self.voucher is None or voucher.cumulative > self.voucher.cumulative:
            self.voucher = pb.Voucher.FromString(voucher.SerializeToString())

    def apply_meter(self, meter: pb.Meter, deposit: int) -> bool:
        validate_message(meter)
        if meter.session_id != self.session_id:
            raise invalid("meter names a different session")
        if meter.seq <= self.meter_seq:
            return False
        if meter.seq != self.meter_seq + 1:
            raise invalid("meter sequence has a gap")
        if any(
            other.context.run_id != meter.run_id
            and other.final is not None
            and other.recovered_through is None
            and other.final[4] == meter.seq
            for other in self.runs.values()
        ):
            raise invalid("meter cursor belongs to another run's definitive final")
        run, value = self._run(meter.run_id), counts(meter)
        catalog = self.catalogs[run.context.rate_card_hash]
        monotone(run.meter_counts, value, catalog, run.context.model)
        if run.final is None:
            self._apply(run, value, deposit)
        else:
            final_counts = TokenCounts(*run.final[:3])
            if meter.seq > run.final[4] or (meter.seq == run.final[4] and value != final_counts):
                raise invalid("meter conflicts with definitive final")
            monotone(value, final_counts, catalog, run.context.model)
        delta_output = value.output_tokens - run.meter_counts.output_tokens
        if run.active:
            self.coverage_numerator += delta_output * catalog.rates_for(run.context.model).output
        run.meter_counts, run.meter_seq = value, meter.seq
        self.meter_seq = meter.seq
        return True

    def apply_final(self, report: pb.Terminal | pb.Receipt, deposit: int) -> bool:
        validate_message(report)
        if report.session_id != self.session_id:
            raise invalid("final names a different session")
        run = self._run(report.run_id)
        if report.model != run.context.model or report.rate_card_hash != run.context.rate_card_hash:
            raise invalid("final differs from accepted run prices")
        value = counts(report)
        amount = (
            priced_numerator(value, self.catalogs[report.rate_card_hash].rates_for(report.model))
            // 1_000_000
        )
        final = (
            value.input_tokens,
            value.cache_tokens,
            value.output_tokens,
            report.status,
            report.final_meter_seq,
            amount,
        )
        if (
            amount != report.amount
            or report.final_meter_seq < run.meter_seq
            or (report.final_meter_seq == run.meter_seq and value != run.meter_counts)
        ):
            raise invalid("final amount or sequence contradicts accepted evidence")
        if report.final_meter_seq and any(
            other.context.run_id != report.run_id
            and (
                other.meter_seq == report.final_meter_seq
                or (
                    other.final is not None
                    and other.recovered_through is None
                    and other.final[4] == report.final_meter_seq
                )
            )
            for other in self.runs.values()
        ):
            raise invalid("final cursor belongs to another run's meter")
        if run.final is not None:
            if run.recovered_through is not None:
                if (
                    run.final[:4] != final[:4]
                    or run.final[5] != final[5]
                    or report.final_meter_seq > run.recovered_through
                ):
                    raise invalid("final contradicts the recovered outcome or cutover")
                run.final, run.recovered_through = final, None
                return False
            if run.final != final:
                raise invalid("conflicting definitive final reports")
            return False
        self._apply(run, value, deposit)
        run.final = final
        return True

    def coverage(self, notice: pb.LowCredit, deposit: int) -> Coverage:
        validate_message(notice)
        if notice.session_id != self.session_id:
            raise invalid("coverage names a different session")
        if not notice.HasField("coverage_request"):
            if not any(
                run.context.model == notice.model
                and run.context.rate_card_hash == notice.rate_card_hash
                for run in self.runs.values()
            ):
                raise invalid("funding guidance has no signed run price binding")
            return Coverage()
        request = notice.coverage_request
        run = self._run(request.run_id)
        if (
            request.control_generation != self.generation
            or run.context.control_generation != self.generation
            or notice.model != run.context.model
            or notice.rate_card_hash != run.context.rate_card_hash
        ):
            if not run.active:
                return Coverage()  # Late guidance for work that already ended.
            raise invalid("coverage correlation differs from signed pending work")
        # Every retained run carries its own signed price acceptance, including one
        # whose stream this agent already ended while the provider still holds it.
        known = {
            (item.context.model, item.context.rate_card_hash) for item in self.runs.values()
        } | set(self.usage)
        rows: dict[tuple[str, bytes], TokenCounts] = {}
        for row in request.required_by_price:
            key = (row.model, row.rate_card_hash)
            if key not in known or key in rows:
                raise invalid("coverage lacks accepted prices or repeats a bucket")
            rows[key] = counts(row)
        candidate = (run.context.model, run.context.rate_card_hash)
        if candidate not in rows:
            raise invalid("coverage omits its candidate")
        priced = {
            key: priced_numerator(value, self.catalogs[key[1]].rates_for(key[0]))
            for key, value in rows.items()
        }
        # Rows are the provider's target at its own check, and it reserves by
        # price: a row may hold fewer tokens of one kind than metered usage
        # (cache beyond a zero-cache bound, input beyond a low estimate) while
        # covering its cost. A final delivered on a Complete stream can also be
        # applied here first, since order across the two streams is unspecified
        # (protocol.md, Definitive outcomes and settlement). Only a bucket priced
        # below its known usage, or a missing one, makes this target superseded:
        # sign nothing for it rather than fail the attachment.
        if not set(self.usage) <= set(rows) or any(
            priced[key] < priced_numerator(value, self.catalogs[key[1]].rates_for(key[0]))
            for key, value in self.usage.items()
        ):
            return Coverage(superseded=True)
        total = sum(priced.values())
        required = required_coverage((total,))
        if required > deposit or not run.active:
            # Ended work is advisory only: its shortfall may still inform the
            # caller's funding (protocol.md, Request admission, step 6).
            return Coverage(required=required)
        current = self.voucher.cumulative if self.voucher is not None else 0
        return Coverage(max(required, current), required, numerator=total)

    def cover(self, coverage: Coverage) -> None:
        """Record the target of a coverage decision the caller is signing."""
        if coverage.target is None or coverage.numerator is None:
            raise invalid("only a signable coverage decision can be recorded")
        self.coverage_numerator = coverage.numerator

    def restore_snapshot(
        self, snapshot: pb.SessionState, *, nonce: bytes, deposit: int, settled: int
    ) -> None:
        validate_message(snapshot)
        if (
            snapshot.session_id != self.session_id
            or snapshot.nonce != nonce
            or descriptor_from_wire(snapshot.descriptor) != self.descriptor
            or snapshot.initial_rate_card_hash != self.initial_hash
        ):
            raise invalid("snapshot identity or initial terms mismatch")
        if snapshot.control_generation <= self.generation or snapshot.meter_seq < self.meter_seq:
            raise invalid("snapshot rolls back generation, cursor or close intent")
        catalogs: dict[bytes, Catalog] = {}
        for version in snapshot.catalogs:
            candidate = parse_catalog(version.catalog)
            if (
                candidate.rate_card_hash != version.rate_card_hash
                or version.rate_card_hash in catalogs
                or self.catalogs.get(version.rate_card_hash) != candidate
            ):
                raise invalid("snapshot catalog has no independently retained publication evidence")
            catalogs[version.rate_card_hash] = candidate
        needed = {self.initial_hash}
        if snapshot.HasField("current_rate_card_hash"):
            needed.add(snapshot.current_rate_card_hash)
        totals: dict[tuple[str, bytes], TokenCounts] = {}
        for row in snapshot.usage_by_price:
            key = (row.model, row.rate_card_hash)
            if key in totals or key[1] not in catalogs:
                raise invalid("duplicate or unpriced usage bucket")
            context = RunContext(
                self.session_id,
                row.acceptance_run_id,
                row.acceptance_control_generation,
                row.model,
                row.rate_card_hash,
            )
            if context.control_generation >= snapshot.control_generation:
                raise invalid("snapshot acceptance is not from a prior generation")
            verify_run_auth(self.domain, self.descriptor, context, row.acceptance_signature)
            if row.acceptance_run_id in self.runs:
                previous_run = self.runs[row.acceptance_run_id]
                if (
                    previous_run.context != context
                    or previous_run.signature != row.acceptance_signature
                ):
                    raise invalid("snapshot price acceptance contradicts retained intent")
            totals[key] = counts(row)
            needed.add(key[1])
            priced_numerator(totals[key], catalogs[key[1]].rates_for(key[0]))
        for key, old in self.usage.items():
            if key not in totals:
                raise invalid("snapshot erased recorded usage")
            monotone(old, totals[key], self.catalogs[key[1]], key[0])
        if snapshot.meter_seq == self.meter_seq and totals != self.usage:
            raise invalid("unchanged snapshot cursor has different usage")
        seen: set[bytes] = set()
        run_updates: list[tuple[Run, TokenCounts, tuple[int, int, int, int, int, int]]] = []
        for row in snapshot.run_states:
            if row.run_id in seen:
                raise invalid("duplicate snapshot run")
            seen.add(row.run_id)
            if row.run_id not in self.runs:
                # A final run this agent already settled and forgot: its counts
                # are in the bucket totals, and only this agent's own signature
                # can name it. Nothing else of it remains to restore.
                if row.status != pb.RUN_STATUS_NOT_ADMITTED:
                    verify_run_auth(
                        self.domain,
                        self.descriptor,
                        RunContext(
                            self.session_id,
                            row.run_id,
                            row.control_generation,
                            row.model,
                            row.rate_card_hash,
                        ),
                        row.auth_signature,
                    )
                continue
            run = self.runs[row.run_id]
            value = counts(row)
            if row.status == pb.RUN_STATUS_NOT_ADMITTED:
                if run.counts != TokenCounts(0, 0, 0) or (
                    run.final is not None and run.final[3] != pb.RUN_STATUS_NOT_ADMITTED
                ):
                    raise invalid("NOT_ADMITTED contradicts recorded work")
                final = (
                    0,
                    0,
                    0,
                    row.status,
                    run.final[4] if run.final is not None else snapshot.meter_seq,
                    0,
                )
            else:
                context = RunContext(
                    self.session_id,
                    row.run_id,
                    row.control_generation,
                    row.model,
                    row.rate_card_hash,
                )
                verify_run_auth(self.domain, self.descriptor, context, row.auth_signature)
                if (
                    context != run.context
                    or row.auth_signature != run.signature
                    or context.rate_card_hash not in catalogs
                ):
                    raise invalid("snapshot run differs from retained intent")
                needed.add(context.rate_card_hash)
                monotone(run.counts, value, catalogs[context.rate_card_hash], context.model)
                key = (context.model, context.rate_card_hash)
                if key not in totals:
                    raise invalid("snapshot run has no usage bucket")
                monotone(value, totals[key], catalogs[key[1]], key[0])
                amount = priced_numerator(value, catalogs[key[1]].rates_for(key[0])) // 1_000_000
                final = (
                    value.input_tokens,
                    value.cache_tokens,
                    value.output_tokens,
                    row.status,
                    run.final[4] if run.final is not None else snapshot.meter_seq,
                    amount,
                )
                if run.final is not None and (run.final[:4] != final[:4] or run.final[5] != amount):
                    raise invalid("recovery contradicts definitive final")
            run_updates.append((run, value, final))
        if not set(self.pending) <= seen or not needed <= set(catalogs):
            raise invalid("snapshot omitted pending outcomes or required catalogs")
        updated = {run.context.run_id: value for run, value, _ in run_updates}
        partition: dict[tuple[str, bytes], TokenCounts] = {}
        for run in self.runs.values():
            value = updated.get(run.context.run_id, run.counts)
            key = (run.context.model, run.context.rate_card_hash)
            old = partition.get(key, TokenCounts(0, 0, 0))
            partition[key] = TokenCounts(
                old.input_tokens + value.input_tokens,
                old.cache_tokens + value.cache_tokens,
                old.output_tokens + value.output_tokens,
            )
        for key, value in partition.items():
            if value != TokenCounts(0, 0, 0):
                if key not in totals:
                    raise invalid("snapshot erased a known run's usage")
                monotone(value, totals[key], catalogs[key[1]], key[0])
        # A forgotten run's counts live only in usage, so the retained runs no
        # longer partition it. Known usage plus each retained run's increase
        # is therefore the floor for the snapshot's totals.
        floor = dict(self.usage)
        for run, value, _ in run_updates:
            key = (run.context.model, run.context.rate_card_hash)
            old = floor.get(key, TokenCounts(0, 0, 0))
            floor[key] = TokenCounts(
                old.input_tokens + value.input_tokens - run.counts.input_tokens,
                old.cache_tokens + value.cache_tokens - run.counts.cache_tokens,
                old.output_tokens + value.output_tokens - run.counts.output_tokens,
            )
        for key, value in floor.items():
            if value != TokenCounts(0, 0, 0):
                if key not in totals:
                    raise invalid("snapshot erased a known run's usage")
                monotone(value, totals[key], self.catalogs[key[1]], key[0])
        total_numerator = sum(
            priced_numerator(value, catalogs[version].rates_for(model))
            for (model, version), value in totals.items()
        )
        amount = cumulative_usage((total_numerator,))
        if amount < settled or snapshot.deposit > deposit or snapshot.settled_cumulative > settled:
            raise invalid("snapshot accounting contradicts confirmed chain totals")
        if not snapshot.HasField("accepted_voucher"):
            if (
                self.voucher is not None
                or total_numerator
                or settled
                or snapshot.usage_by_price
                or any(row.status != pb.RUN_STATUS_NOT_ADMITTED for row in snapshot.run_states)
            ):
                raise invalid("snapshot erased accepted authorization")
        else:
            previous = self.voucher
            incoming = snapshot.accepted_voucher
            verify_voucher(self.domain, self.descriptor, incoming.cumulative, incoming.signature)
            if (
                incoming.session_id != self.session_id
                or incoming.cumulative > deposit
                or incoming.cumulative * 1_000_000 < total_numerator
                or (previous is not None and incoming.cumulative < previous.cumulative)
            ):
                raise invalid("snapshot voucher contradicts authorization")
        self.usage = totals
        for run, value, final in run_updates:
            if run.final is None:
                run.recovered_through = snapshot.meter_seq
            run.counts, run.meter_counts, run.final = value, value, final
        if snapshot.HasField("accepted_voucher"):
            self.accept_voucher(snapshot.accepted_voucher, deposit)
        self.meter_seq, self.generation = snapshot.meter_seq, snapshot.control_generation
        self.close_pending = self.close_pending or snapshot.close_pending
        self.ready = snapshot.ready and not self.close_pending
        self.coverage_numerator = total_numerator
