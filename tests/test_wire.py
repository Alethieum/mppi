"""Generated-wire conformance, not an implementation of paid session behavior.

The loopback servicers below only exercise generated bindings and native gRPC
framing. They do not verify signatures, admit work, meter usage or access chain
state. Semantic validation is a separate SDK responsibility.
"""

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import grpc
import pytest
from google.protobuf import any_pb2, descriptor_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.rpc import status_pb2
from mppi.v1 import mppi_pb2 as wire
from mppi.v1 import mppi_pb2_grpc as services

MAX_UINT64 = (1 << 64) - 1
SESSION_ID = bytes(range(32))
RUN_ID = bytes(reversed(range(32)))
CATALOG_HASH = bytes([0xA5]) * 32
NONCE = bytes([0xB6]) * 32
# Synthetic fixed-width bytes are suitable only for encoding fixtures.
SIGNATURE = bytes([0x11]) * 32 + bytes([0x22]) * 32 + bytes([27])
PAYLOAD = b'{"model":"example/model","stream":true,"seed":9007199254740993,"custom":{"n":1e1000}}'


def request() -> wire.CompleteRequest:
    return wire.CompleteRequest(
        model="example/model",
        stream=True,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        control_generation=1,
        rate_card_hash=CATALOG_HASH,
        auth_signature=SIGNATURE,
        payload=PAYLOAD,
    )


def test_descriptor_has_exact_public_services_and_stream_shapes() -> None:
    descriptor = wire.DESCRIPTOR
    assert descriptor.package == "mppi.v1"
    assert len(descriptor.message_types_by_name) == 28
    assert len(descriptor.enum_types_by_name) == 5
    assert len(descriptor.dependencies) == 0
    methods = {
        f"/{service.full_name}/{method.name}": (
            method.client_streaming,
            method.server_streaming,
            method.input_type.full_name,
            method.output_type.full_name,
        )
        for service in descriptor.services_by_name.values()
        for method in service.methods
    }
    assert methods == {
        "/mppi.v1.Chat/Completions": (False, True, "mppi.v1.CompleteRequest", "mppi.v1.ChatChunk"),
        "/mppi.v1.Control/Session": (
            True,
            True,
            "mppi.v1.ControlMessage",
            "mppi.v1.ControlMessage",
        ),
    }


def test_checked_in_descriptor_and_generated_bindings_agree() -> None:
    descriptor_path = Path(__file__).resolve().parents[1] / "proto" / "mppi.v1.binpb"
    files = descriptor_pb2.FileDescriptorSet.FromString(descriptor_path.read_bytes()).file
    assert [file.name for file in files] == ["mppi/v1/mppi.proto"]
    generated = descriptor_pb2.FileDescriptorProto()
    wire.DESCRIPTOR.CopyToProto(generated)

    # Source location comments and inferred JSON names may be omitted from the
    # generated runtime's compact descriptor; compare the actual wire ABI.
    def abi(file: descriptor_pb2.FileDescriptorProto) -> tuple:
        return (
            file.package,
            file.syntax,
            tuple(
                (
                    message.name,
                    tuple(
                        (
                            field.name,
                            field.number,
                            field.type,
                            field.type_name,
                            field.label,
                            field.proto3_optional,
                            field.oneof_index if field.HasField("oneof_index") else None,
                        )
                        for field in message.field
                    ),
                )
                for message in file.message_type
            ),
            tuple(service.SerializeToString() for service in file.service),
            tuple(enum.SerializeToString() for enum in file.enum_type),
        )

    assert abi(files[0]) == abi(generated)


@pytest.mark.parametrize(
    ("message", "names"),
    [
        (
            wire.CompleteRequest,
            [
                "model",
                "stream",
                "session_id",
                "run_id",
                "control_generation",
                "rate_card_hash",
                "auth_signature",
                "payload",
            ],
        ),
        (
            wire.Terminal,
            [
                "input_tokens",
                "cache_tokens",
                "output_tokens",
                "amount",
                "session_id",
                "run_id",
                "model",
                "rate_card_hash",
                "status",
                "final_meter_seq",
                "settlement_reference",
            ],
        ),
        (
            wire.Receipt,
            [
                "session_id",
                "run_id",
                "model",
                "rate_card_hash",
                "input_tokens",
                "output_tokens",
                "amount",
                "settled_cumulative",
                "cache_tokens",
                "status",
                "final_meter_seq",
                "chain_observation",
                "settlement_reference",
            ],
        ),
        (
            wire.ControlMessage,
            [
                "challenge_request",
                "challenge",
                "session_open",
                "reconnect_challenge",
                "reconnect_request",
                "session_state",
                "voucher",
                "meter",
                "top_up_notice",
                "low_credit",
                "drain",
                "receipt",
                "close_request",
                "close_result",
            ],
        ),
        (
            wire.SessionDescriptor,
            ["agent", "session_signer", "provider_id", "recipient", "operator", "token", "salt"],
        ),
        (wire.TopUpNotice, ["session_id", "tx", "model", "rate_card_hash", "tokens"]),
        (
            wire.LowCredit,
            [
                "session_id",
                "model",
                "rate_card_hash",
                "remaining_tokens",
                "suggest_top_up_tokens",
                "coverage_request",
            ],
        ),
    ],
)
def test_public_tag_allocations(message: type, names: list[str]) -> None:
    assert {field.name: field.number for field in message.DESCRIPTOR.fields} == {
        name: tag for tag, name in enumerate(names, start=1)
    }


def test_every_singular_field_tracks_presence() -> None:
    for message in wire.DESCRIPTOR.message_types_by_name.values():
        for field in message.fields:
            if not field.is_repeated:
                assert field.has_presence, f"{message.name}.{field.name}"
    assert (
        wire.CompleteRequest.DESCRIPTOR.fields_by_name["payload"].type == FieldDescriptor.TYPE_BYTES
    )
    assert wire.ChatChunk.DESCRIPTOR.fields_by_name["payload"].type == FieldDescriptor.TYPE_BYTES


def test_present_zero_and_false_do_not_collapse_into_absence() -> None:
    missing = wire.CompleteRequest()
    explicit = wire.CompleteRequest(stream=False, control_generation=0)
    decoded = wire.CompleteRequest.FromString(explicit.SerializeToString())
    assert not missing.HasField("stream")
    assert decoded.HasField("stream") and decoded.stream is False
    assert decoded.HasField("control_generation") and decoded.control_generation == 0
    # The decoded zero generation remains semantically invalid for admission.
    missing_voucher = wire.Voucher()
    zero_voucher = wire.Voucher(cumulative=0)
    assert not missing_voucher.HasField("cumulative")
    assert wire.Voucher.FromString(zero_voucher.SerializeToString()).HasField("cumulative")
    error = wire.PaymentError(pause_level=wire.PAUSE_LEVEL_NONE)
    assert wire.PaymentError.FromString(error.SerializeToString()).HasField("pause_level")


def test_original_json_bytes_preserve_large_integer_and_numeric_text() -> None:
    original = request()
    decoded = wire.CompleteRequest.FromString(original.SerializeToString())
    assert decoded.payload == PAYLOAD
    # Parse only this assertion's integer; the bytes remain the transport value.
    parsed = json.loads(decoded.payload, parse_float=str)
    assert parsed["seed"] == 9007199254740993
    assert parsed["custom"]["n"] == "1e1000"
    chunk = wire.ChatChunk(payload=decoded.payload)
    assert wire.ChatChunk.FromString(chunk.SerializeToString()).payload == PAYLOAD


@pytest.mark.parametrize("amount", [0, (1 << 53) + 1, MAX_UINT64])
def test_uint64_values_round_trip_without_numeric_coercion(amount: int) -> None:
    voucher = wire.Voucher(session_id=SESSION_ID, cumulative=amount, signature=SIGNATURE)
    assert wire.Voucher.FromString(voucher.SerializeToString()).cumulative == amount
    meter = wire.Meter(input_tokens=amount, cache_tokens=0, output_tokens=amount)
    result = wire.Meter.FromString(meter.SerializeToString())
    assert (result.input_tokens, result.output_tokens) == (amount, amount)


@pytest.mark.parametrize("amount", [-1, MAX_UINT64 + 1])
def test_generated_uint64_assignment_rejects_out_of_range(amount: int) -> None:
    with pytest.raises(ValueError):
        wire.Voucher(cumulative=amount)


def test_fixed_width_descriptor_and_pending_run_fixtures() -> None:
    provider_id = (1 << 255) + (1 << 64) + 7
    descriptor = wire.SessionDescriptor(
        agent=bytes([1]) * 20,
        session_signer=bytes([2]) * 20,
        provider_id=provider_id.to_bytes(32, "big"),
        recipient=bytes([3]) * 20,
        operator=bytes([4]) * 20,
        token=bytes.fromhex("3600000000000000000000000000000000000000"),
        salt=SESSION_ID,
    )
    decoded = wire.SessionDescriptor.FromString(descriptor.SerializeToString())
    assert int.from_bytes(decoded.provider_id, "big") == provider_id
    assert len(decoded.provider_id) == len(decoded.salt) == 32
    for field in ("agent", "session_signer", "recipient", "operator", "token"):
        assert len(getattr(decoded, field)) == 20
    pending_ids = [value.to_bytes(32, "big") for value in range(1, 1025)]
    reconnect = wire.ReconnectRequest(
        session_id=SESSION_ID,
        nonce=NONCE,
        signature=SIGNATURE,
        last_meter_seq=0,
        pending_run_ids=pending_ids,
    )
    assert (
        list(wire.ReconnectRequest.FromString(reconnect.SerializeToString()).pending_run_ids)
        == pending_ids
    )
    assert len(wire.ControlMessage(reconnect_request=reconnect).SerializeToString()) < 64 * 1024


def test_generated_decode_does_not_claim_semantic_validation() -> None:
    # A bytes32 width, known enum and selected oneof require SDK validation.
    malformed_width = wire.Voucher(session_id=b"short", cumulative=0)
    assert wire.Voucher.FromString(malformed_width.SerializeToString()).session_id == b"short"
    unknown_enum = wire.Terminal(status=99)
    assert wire.Terminal.FromString(unknown_enum.SerializeToString()).status == 99
    # Unknown length-delimited field 100 is not a known ChatChunk variant.
    assert wire.ChatChunk.FromString(b"\xa2\x06\x00").WhichOneof("kind") is None
    # Standard protobuf duplicate scalar semantics are last-value-wins.
    assert wire.CompleteRequest.FromString(b"\x10\x00\x10\x01").stream is True
    # Unknown ordinary field 999 survives without changing known data.
    original = request()
    decoded = wire.CompleteRequest.FromString(original.SerializeToString() + b"\xb8\x3e\x01")
    assert decoded.payload == original.payload and decoded.run_id == original.run_id


def test_oneof_replacement_and_zero_scalar_presence() -> None:
    message = wire.ChatChunk(payload=b"{}")
    assert message.WhichOneof("kind") == "payload"
    message.terminal.CopyFrom(wire.Terminal(status=wire.RUN_STATUS_COMPLETED, amount=0))
    result = wire.ChatChunk.FromString(message.SerializeToString())
    assert result.WhichOneof("kind") == "terminal"
    assert result.payload == b""
    assert result.terminal.HasField("amount") and result.terminal.amount == 0


def payment_status() -> status_pb2.Status:
    detail = wire.PaymentError(
        code=wire.PAYMENT_ERROR_CODE_SESSION_NOT_READY,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        reason="AUTHORIZATION_REQUIRED",
    )
    wrapped = any_pb2.Any()
    wrapped.Pack(detail)
    return status_pb2.Status(code=9, message="Authorization required", details=[wrapped])


def check_payment_status(encoded: bytes) -> None:
    status = status_pb2.Status.FromString(encoded)
    assert status.code == 9
    assert len(status.details) == 1
    assert status.details[0].type_url == "type.googleapis.com/mppi.v1.PaymentError"
    detail = wire.PaymentError()
    assert status.details[0].Unpack(detail)
    assert detail.code == wire.PAYMENT_ERROR_CODE_SESSION_NOT_READY
    assert detail.reason == "AUTHORIZATION_REQUIRED"
    assert (detail.session_id, detail.run_id) == (SESSION_ID, RUN_ID)


def test_payment_error_any_interoperability() -> None:
    encoded = payment_status().SerializeToString()
    assert len(encoded) <= 2048
    check_payment_status(encoded)


def test_generated_native_grpc_services_and_rich_trailers() -> None:
    """Exercise native method routing and both stream shapes on loopback only."""
    release_control = Event()
    paths: list[str] = []

    class RecordMethods(grpc.ServerInterceptor):
        def intercept_service(self, continuation, handler_call_details):
            paths.append(handler_call_details.method)
            return continuation(handler_call_details)

    class ChatEncodingServicer(services.ChatServicer):
        def Completions(self, incoming, context):
            if incoming.model == "example/error":
                status = payment_status()
                context.set_trailing_metadata(
                    (("grpc-status-details-bin", status.SerializeToString()),)
                )
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, status.message)
            yield wire.ChatChunk(payload=incoming.payload)
            yield wire.ChatChunk(
                terminal=wire.Terminal(
                    input_tokens=0,
                    cache_tokens=0,
                    output_tokens=0,
                    amount=0,
                    session_id=incoming.session_id,
                    run_id=incoming.run_id,
                    model=incoming.model,
                    rate_card_hash=incoming.rate_card_hash,
                    status=wire.RUN_STATUS_COMPLETED,
                    final_meter_seq=1,
                )
            )

    class ControlEncodingServicer(services.ControlServicer):
        def Session(self, incoming, context) -> Iterator[wire.ControlMessage]:
            yield wire.ControlMessage(reconnect_challenge=wire.ReconnectChallenge(nonce=NONCE))
            for item in incoming:
                assert item.WhichOneof("kind") == "challenge_request"
                yield wire.ControlMessage(
                    challenge=wire.Challenge(
                        request_id=item.challenge_request.request_id,
                        model=item.challenge_request.model,
                        rate_card_hash=CATALOG_HASH,
                        provider_id=(7).to_bytes(32, "big"),
                        contract=bytes([5]) * 20,
                        chain_id=5042002,
                        operator=bytes([4]) * 20,
                        cover_output_tokens=1000,
                        suggest_session_tokens=1_000_000,
                    )
                )
            if not release_control.wait(timeout=5):
                context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Test Control bound")

    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool, interceptors=(RecordMethods(),))
        services.add_ChatServicer_to_server(ChatEncodingServicer(), server)
        services.add_ControlServicer_to_server(ControlEncodingServicer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        assert port > 0
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                control = services.ControlStub(channel).Session(
                    iter(
                        [
                            wire.ControlMessage(
                                challenge_request=wire.ChallengeRequest(
                                    request_id=RUN_ID, model="example/model"
                                )
                            )
                        ]
                    ),
                    timeout=5,
                )
                assert next(control).reconnect_challenge.nonce == NONCE
                assert next(control).challenge.request_id == RUN_ID
                # Control remains open while a separate Complete RPC returns.
                call = services.ChatStub(channel).Completions(request(), timeout=5)
                output = list(call)
                assert [item.WhichOneof("kind") for item in output] == ["payload", "terminal"]
                assert output[0].payload == PAYLOAD
                assert output[1].terminal.run_id == RUN_ID
                assert call.code() is grpc.StatusCode.OK

                failed_request = request()
                failed_request.model = "example/error"
                failed_request.payload = b'{"model":"example/error","stream":true}'
                with pytest.raises(grpc.RpcError) as caught:
                    list(services.ChatStub(channel).Completions(failed_request, timeout=5))
                assert caught.value.code() is grpc.StatusCode.FAILED_PRECONDITION
                metadata = dict(caught.value.trailing_metadata())
                check_payment_status(metadata["grpc-status-details-bin"])
                release_control.set()
                assert list(control) == []
                assert control.code() is grpc.StatusCode.OK
        finally:
            release_control.set()
            server.stop(grace=0).wait(timeout=5)
    assert paths.count("/mppi.v1.Chat/Completions") == 2
    assert paths.count("/mppi.v1.Control/Session") == 1
