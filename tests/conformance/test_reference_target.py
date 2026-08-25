# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage.streaming import (
    CursorExpired,
    DeliveryRejected,
    GroupPositionConflict,
    InvalidCursor,
    InvalidStreamingDefinition,
    StreamingPolicyDenied,
)
from meridian_storage.streaming.testing import (
    ConformanceRange,
    InMemoryStreamingTarget,
    run_streaming_conformance,
)


@pytest.mark.conformance
def test_reference_target_passes_every_mandatory_case() -> None:
    report = run_streaming_conformance(InMemoryStreamingTarget())
    assert report.passed
    assert tuple(case.name for case in report.cases) == (
        "method-semantics",
        "per-logical-partition-ordering",
        "at-least-once-redelivery",
        "acknowledgement-recovery",
        "replay-and-group-transition",
        "cursor-validation-and-expiry",
        "tenancy-policy-audit-lineage-telemetry",
        "normalized-errors",
    )
    assert report.fingerprint.startswith("sha256:")
    assert report.to_dict()["passed"] is True


@pytest.mark.conformance
def test_runner_rejects_targets_without_the_bridge_contract() -> None:
    with pytest.raises(TypeError):
        run_streaming_conformance(object())  # type: ignore[arg-type]


@pytest.mark.conformance
def test_reference_target_normalizes_cursor_delivery_and_cas_failures() -> None:
    target = InMemoryStreamingTarget()
    with pytest.raises(DeliveryRejected):
        target.acknowledge("unknown")
    with pytest.raises(InvalidCursor):
        target.read_range("unknown")
    target.publish(event_id="one", logical_partition="a", sequence=1, tenant="tenant-a")
    cursor = target.read_range(limit=1).next_cursor
    assert cursor is not None
    with pytest.raises(GroupPositionConflict):
        target.transition_group_position(cursor, f"sha256:{'0' * 64}")
    target.expire_cursor(cursor)
    with pytest.raises(CursorExpired):
        target.replay(cursor)


@pytest.mark.conformance
def test_reference_target_defensive_lifecycle_failures() -> None:
    target = InMemoryStreamingTarget()
    with pytest.raises(StreamingPolicyDenied, match="tenant scope"):
        target.publish(event_id="one", logical_partition="a", sequence=1, tenant="")
    with pytest.raises(DeliveryRejected):
        target.negative_acknowledge("unknown")
    with pytest.raises(InvalidStreamingDefinition):
        target.read_range(limit=0)
    with pytest.raises(InvalidCursor):
        target.expire_cursor("unknown")

    target.publish(event_id="one", logical_partition="a", sequence=1, tenant="tenant-a")
    delivery = target.poll(limit=1)[0]
    assert target.poll(limit=1) == ()
    target.acknowledge(delivery.token)
    assert target.poll(limit=1) == ()
    cursor = target.read_range(limit=1).next_cursor
    assert cursor is not None
    with pytest.raises(InvalidCursor):
        target.transition_group_position("unknown", target.group_position_fingerprint())
    target.expire_cursor(cursor)
    with pytest.raises(CursorExpired):
        target.transition_group_position(cursor, target.group_position_fingerprint())


class _ReverseTarget(InMemoryStreamingTarget):
    def poll(self, *, limit: int = 100):
        return tuple(reversed(super().poll(limit=limit)))


class _NoCursorTarget(InMemoryStreamingTarget):
    def read_range(self, cursor: str | None = None, *, limit: int = 100) -> ConformanceRange:
        page = super().read_range(cursor, limit=limit)
        return ConformanceRange(page.event_ids, None, page.truncated)


@pytest.mark.conformance
@pytest.mark.parametrize("target", [_ReverseTarget(), _NoCursorTarget()])
def test_runner_rejects_semantically_nonconforming_targets(target) -> None:
    with pytest.raises(AssertionError, match="conformance failed"):
        run_streaming_conformance(target)
