# SPDX-License-Identifier: Apache-2.0
"""Reusable black-box conformance suite for Streaming providers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from ..canonical import JsonValue, sha256_fingerprint
from ..catalog import StreamingCatalogProvider
from ..errors import (
    CursorExpired,
    DeliveryRejected,
    InvalidStreamingDefinition,
    StreamingPolicyDenied,
)
from ..operations import GroupPositionTransition, ReplayOperation

_SECOND_DELIVERY_ATTEMPT = 2


@dataclass(frozen=True, slots=True)
class ConformanceDelivery:
    event_id: str
    logical_partition: str
    sequence: int
    token: str
    attempt: int


@dataclass(frozen=True, slots=True)
class ConformanceRange:
    event_ids: tuple[str, ...]
    next_cursor: str | None
    truncated: bool = False


@runtime_checkable
class StreamingConformanceTarget(Protocol):
    """Small provider-owned bridge consumed by the released conformance runner."""

    @property
    def target_id(self) -> str: ...

    @property
    def capability_fingerprint(self) -> str: ...

    def reset(self) -> None: ...

    def publish(
        self,
        *,
        event_id: str,
        logical_partition: str,
        sequence: int,
        tenant: str,
        authorized: bool = True,
    ) -> None: ...

    def poll(self, *, limit: int = 100) -> tuple[ConformanceDelivery, ...]: ...

    def acknowledge(self, token: str) -> None: ...

    def negative_acknowledge(self, token: str) -> None: ...

    def recover(self) -> None: ...

    def read_range(self, cursor: str | None = None, *, limit: int = 100) -> ConformanceRange: ...

    def replay(self, cursor: str) -> ConformanceRange: ...

    def group_position_fingerprint(self) -> str: ...

    def transition_group_position(self, cursor: str, expected_fingerprint: str) -> None: ...

    def expire_cursor(self, cursor: str) -> None: ...

    def evidence_kinds(self) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class ConformanceCase:
    name: str
    passed: bool
    evidence: Mapping[str, JsonValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "passed": self.passed, "evidence": dict(self.evidence)}


@dataclass(frozen=True, slots=True)
class StreamingConformanceReport:
    target_id: str
    capability_fingerprint: str
    cases: tuple[ConformanceCase, ...]
    contract_version: str = "meridian.streaming.conformance.v1"

    @property
    def passed(self) -> bool:
        return bool(self.cases) and all(case.passed for case in self.cases)

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.contract_version,
            "targetId": self.target_id,
            "capabilityFingerprint": self.capability_fingerprint,
            "passed": self.passed,
            "cases": [case.to_dict() for case in self.cases],
        }


def run_streaming_conformance(target: StreamingConformanceTarget) -> StreamingConformanceReport:
    """Exercise mandatory V1 semantics and return deterministic release evidence."""

    if not isinstance(target, StreamingConformanceTarget):
        raise TypeError("target does not implement StreamingConformanceTarget")
    cases = (
        _case_method_semantics(),
        _case_ordering(target),
        _case_redelivery(target),
        _case_acknowledgement_recovery(target),
        _case_replay_and_group_transition(target),
        _case_cursor_expiry(target),
        _case_hooks_and_policy(target),
        _case_normalized_errors(target),
    )
    report = StreamingConformanceReport(
        target_id=target.target_id,
        capability_fingerprint=target.capability_fingerprint,
        cases=cases,
    )
    if not report.passed:
        failed = [case.name for case in cases if not case.passed]
        raise AssertionError(f"Streaming conformance failed: {failed!r}")
    return report


def _case_method_semantics() -> ConformanceCase:
    provider = StreamingCatalogProvider()
    surface = provider.create_surface()
    expression = surface.publish(
        resource="conformance.events",
        data={"eventId": "probe", "data": {"value": 1}},
        idempotency_key="conformance-probe",
    )
    operation = provider.normalize(expression)
    passed = (
        operation.operation_contract == "meridian.streaming.publish"
        and operation.idempotent
        and not operation.read_only
        and operation.resources[0].logical_name == "conformance.events"
    )
    return ConformanceCase(
        "method-semantics",
        passed,
        {"operationFingerprint": operation.request_fingerprint},
    )


def _case_ordering(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="a-1", logical_partition="a", sequence=1, tenant="tenant-a")
    target.publish(event_id="b-1", logical_partition="b", sequence=1, tenant="tenant-a")
    target.publish(event_id="a-2", logical_partition="a", sequence=2, tenant="tenant-a")
    deliveries = target.poll()
    by_partition: dict[str, list[int]] = {}
    for delivery in deliveries:
        by_partition.setdefault(delivery.logical_partition, []).append(delivery.sequence)
    passed = bool(deliveries) and all(values == sorted(values) for values in by_partition.values())
    return ConformanceCase(
        "per-logical-partition-ordering",
        passed,
        {"deliveryCount": len(deliveries)},
    )


def _case_redelivery(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="redelivery", logical_partition="a", sequence=1, tenant="tenant-a")
    first = target.poll(limit=1)[0]
    target.negative_acknowledge(first.token)
    second = target.poll(limit=1)[0]
    passed = (
        first.event_id == second.event_id
        and first.attempt == 1
        and second.attempt == _SECOND_DELIVERY_ATTEMPT
    )
    return ConformanceCase("at-least-once-redelivery", passed, {"attempt": second.attempt})


def _case_acknowledgement_recovery(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="acked", logical_partition="a", sequence=1, tenant="tenant-a")
    delivery = target.poll(limit=1)[0]
    target.acknowledge(delivery.token)
    target.recover()
    passed = target.poll(limit=1) == ()
    return ConformanceCase("acknowledgement-recovery", passed, {})


def _case_replay_and_group_transition(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="replay-1", logical_partition="a", sequence=1, tenant="tenant-a")
    target.publish(event_id="replay-2", logical_partition="a", sequence=2, tenant="tenant-a")
    page = target.read_range(limit=1)
    cursor = page.next_cursor
    if cursor is None:
        return ConformanceCase("replay-and-group-transition", False, {})
    before = target.group_position_fingerprint()
    replayed = target.replay(cursor)
    unchanged = target.group_position_fingerprint() == before
    target.transition_group_position(cursor, before)
    changed = target.group_position_fingerprint() != before
    replay_contract = ReplayOperation(stream="conformance.events", start=cursor).to_operation()
    group_contract = GroupPositionTransition(
        subscription="conformance.subscription",
        consumer_group="conformance.group",
        position=cursor,
        expected_position_fingerprint=f"sha256:{'0' * 64}",
        authorization_ref="conformance-policy",
        reason="conformance",
    ).to_operation()
    passed = (
        unchanged
        and changed
        and bool(replayed.event_ids)
        and replay_contract.read_only
        and not group_contract.read_only
    )
    return ConformanceCase(
        "replay-and-group-transition", passed, {"replayed": len(replayed.event_ids)}
    )


def _case_cursor_expiry(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="cursor", logical_partition="a", sequence=1, tenant="tenant-a")
    cursor = target.read_range(limit=1).next_cursor
    if cursor is None:
        return ConformanceCase("cursor-validation-and-expiry", False, {})
    target.expire_cursor(cursor)
    try:
        target.read_range(cursor)
    except CursorExpired as exc:
        return ConformanceCase(
            "cursor-validation-and-expiry",
            exc.code == "MERIDIAN_STREAMING_CURSOR_EXPIRED",
            {"errorCode": exc.code},
        )
    return ConformanceCase("cursor-validation-and-expiry", False, {})


def _case_hooks_and_policy(target: StreamingConformanceTarget) -> ConformanceCase:
    target.reset()
    target.publish(event_id="hooks", logical_partition="a", sequence=1, tenant="tenant-a")
    kinds = set(target.evidence_kinds())
    denied = False
    try:
        target.publish(
            event_id="denied",
            logical_partition="a",
            sequence=2,
            tenant="tenant-b",
            authorized=False,
        )
    except StreamingPolicyDenied:
        denied = True
    passed = denied and {"audit", "lineage", "telemetry"} <= kinds
    return ConformanceCase(
        "tenancy-policy-audit-lineage-telemetry",
        passed,
        {"evidenceKinds": tuple(sorted(kinds))},
    )


def _case_normalized_errors(target: StreamingConformanceTarget) -> ConformanceCase:
    delivery_error = False
    limit_error = False
    try:
        target.acknowledge("unknown-token")
    except DeliveryRejected as exc:
        delivery_error = exc.code == "MERIDIAN_STREAMING_DELIVERY_INVALID"
    try:
        target.poll(limit=0)
    except InvalidStreamingDefinition as exc:
        limit_error = exc.code == "MERIDIAN_STREAMING_INVALID_DEFINITION"
    return ConformanceCase("normalized-errors", delivery_error and limit_error, {})


__all__ = [
    "ConformanceCase",
    "ConformanceDelivery",
    "ConformanceRange",
    "StreamingConformanceReport",
    "StreamingConformanceTarget",
    "run_streaming_conformance",
]
