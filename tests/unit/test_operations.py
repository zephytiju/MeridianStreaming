# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage import Operation
from meridian_storage.streaming import (
    GROUP_POSITION_OPERATION_CONTRACT,
    REPLAY_OPERATION_CONTRACT,
    GroupPositionTransition,
    ReplayOperation,
)


def test_replay_is_read_only_and_does_not_mutate_group() -> None:
    operation = ReplayOperation(
        stream="orders.events",
        start="opaque-start",
        end="opaque-end",
        limit=500,
        authorization_ref="retention-policy",
        reason="recovery",
    ).to_operation()
    assert operation.operation_contract == REPLAY_OPERATION_CONTRACT
    assert operation.read_only and operation.idempotent
    assert operation.input["mutatesConsumerGroup"] is False
    assert Operation.from_mapping(operation.to_dict()) == operation


def test_group_transition_is_compare_and_set() -> None:
    operation = GroupPositionTransition(
        subscription="orders.subscription",
        consumer_group="orders.workers",
        position="opaque-position",
        expected_position_fingerprint=f"sha256:{'a' * 64}",
        authorization_ref="admin-policy",
        reason="approved-recovery",
    ).to_operation()
    assert operation.operation_contract == GROUP_POSITION_OPERATION_CONTRACT
    assert not operation.read_only and operation.idempotent
    assert operation.input["expectedPositionFingerprint"] == f"sha256:{'a' * 64}"


@pytest.mark.parametrize("limit", [0, 10_001, True])
def test_replay_limit_validation(limit: int) -> None:
    with pytest.raises(ValueError):
        ReplayOperation(stream="orders.events", start="opaque", limit=limit).to_operation()


def test_group_transition_requires_fingerprint() -> None:
    with pytest.raises(ValueError):
        GroupPositionTransition(
            subscription="orders.subscription",
            consumer_group="orders.workers",
            position="opaque",
            expected_position_fingerprint="not-a-fingerprint",
            authorization_ref="admin",
            reason="test",
        ).to_operation()


def test_explicit_operations_accept_opaque_mapping_data() -> None:
    replay = ReplayOperation(
        stream="orders.events",
        start={"value": "opaque-start"},
        end={"value": "opaque-end"},
    ).to_operation()
    assert replay.input["start"] == {"value": "opaque-start"}
    with pytest.raises(TypeError, match="opaque token"):
        ReplayOperation(stream="orders.events", start=3).to_operation()  # type: ignore[arg-type]
