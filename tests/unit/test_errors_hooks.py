# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from meridian_storage import Operation, OperationContext
from meridian_storage.streaming import (
    AcknowledgementConflict,
    CursorExpired,
    DeliveryRejected,
    DeliveryTimeout,
    EvidenceKind,
    GroupPositionConflict,
    InvalidCursor,
    InvalidEvent,
    InvalidStreamingDefinition,
    MigrationRequired,
    RebalanceConflict,
    RetentionBoundaryExceeded,
    StreamingCapabilityMismatch,
    StreamingCatalogProvider,
    StreamingEvidence,
    StreamingHooks,
    StreamingPolicyDenied,
    StreamingResourceNotFound,
    StreamingUnavailable,
)


@dataclass
class Policy:
    calls: int = 0

    def authorize(self, context: OperationContext, operation: Operation) -> None:
        self.calls += 1


@dataclass
class Sink:
    kinds: list[EvidenceKind] = field(default_factory=list)

    def emit(self, context: OperationContext, evidence: StreamingEvidence) -> None:
        self.kinds.append(evidence.kind)


def test_hooks_route_policy_and_evidence() -> None:
    policy = Policy()
    audit, lineage, telemetry = Sink(), Sink(), Sink()
    hooks = StreamingHooks(policy, audit, lineage, telemetry)
    operation = StreamingCatalogProvider().normalize(
        StreamingCatalogProvider()
        .create_surface()
        .publish(resource="orders.events", data={"eventId": "one"})
    )
    context = OperationContext(principal_ref="identity:user/1", tenant="tenant-a")
    hooks.authorize(context, operation)
    evidence = StreamingEvidence(
        EvidenceKind.AUDIT,
        operation.operation_contract,
        "2026-08-25T12:00:00Z",
        "accepted",
        {"resource": "orders.events"},
    )
    hooks.emit(context, evidence)
    assert policy.calls == 1
    assert audit.kinds == [EvidenceKind.AUDIT]
    assert evidence.to_dict()["outcome"] == "accepted"


def test_stable_error_envelopes() -> None:
    invalid = InvalidStreamingDefinition(
        "bad input", requirement="expression.arguments", logical_references=("b", "a", "a")
    )
    assert invalid.to_dict()["logicalReferences"] == ["a", "b"]
    assert CursorExpired().category.value == "COMPATIBILITY"
    assert StreamingPolicyDenied("denied").category.value == "AUTHORIZATION"
    assert StreamingUnavailable("down").retryable


@pytest.mark.parametrize(
    ("error_type", "code"),
    [
        (InvalidStreamingDefinition, "MERIDIAN_STREAMING_INVALID_DEFINITION"),
        (InvalidEvent, "MERIDIAN_STREAMING_EVENT_INVALID"),
        (InvalidCursor, "MERIDIAN_STREAMING_CURSOR_INVALID"),
        (CursorExpired, "MERIDIAN_STREAMING_CURSOR_EXPIRED"),
        (DeliveryRejected, "MERIDIAN_STREAMING_DELIVERY_INVALID"),
        (DeliveryTimeout, "MERIDIAN_STREAMING_DELIVERY_TIMEOUT"),
        (AcknowledgementConflict, "MERIDIAN_STREAMING_ACKNOWLEDGEMENT_CONFLICT"),
        (GroupPositionConflict, "MERIDIAN_STREAMING_GROUP_POSITION_CONFLICT"),
        (RebalanceConflict, "MERIDIAN_STREAMING_REBALANCE_CONFLICT"),
        (RetentionBoundaryExceeded, "MERIDIAN_STREAMING_RETENTION_BOUNDARY"),
        (StreamingCapabilityMismatch, "MERIDIAN_STREAMING_CAPABILITY_MISMATCH"),
        (StreamingResourceNotFound, "MERIDIAN_STREAMING_RESOURCE_NOT_FOUND"),
        (StreamingPolicyDenied, "MERIDIAN_STREAMING_POLICY_DENIED"),
        (StreamingUnavailable, "MERIDIAN_STREAMING_UNAVAILABLE"),
        (MigrationRequired, "MERIDIAN_STREAMING_MIGRATION_REQUIRED"),
    ],
)
def test_every_normalized_error_has_a_stable_code(error_type, code: str) -> None:
    error = error_type("safe message")
    assert error.code == code
    assert error.to_dict()["code"] == code


def test_error_details_reject_unsafe_shapes() -> None:
    with pytest.raises(TypeError, match="requirement"):
        InvalidStreamingDefinition("bad", requirement=3)
    with pytest.raises(TypeError, match="logical_references"):
        InvalidStreamingDefinition("bad", logical_references=["orders.events"])


def test_evidence_rejects_non_mapping_attributes() -> None:
    with pytest.raises(TypeError, match="object"):
        StreamingEvidence(
            EvidenceKind.AUDIT,
            "meridian.streaming.publish",
            "2026-08-25T12:00:00Z",
            "accepted",
            [],  # type: ignore[arg-type]
        )
