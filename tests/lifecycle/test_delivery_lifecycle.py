# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage.streaming import StreamingPolicyDenied
from meridian_storage.streaming.testing import InMemoryStreamingTarget


@pytest.mark.lifecycle
def test_unacknowledged_delivery_is_recovered_and_redelivered() -> None:
    target = InMemoryStreamingTarget()
    target.publish(event_id="one", logical_partition="a", sequence=1, tenant="tenant-a")
    first = target.poll(limit=1)[0]
    target.recover()
    second = target.poll(limit=1)[0]
    assert second.event_id == first.event_id
    assert second.attempt == 2


@pytest.mark.lifecycle
def test_acknowledgement_is_durable_across_recovery() -> None:
    target = InMemoryStreamingTarget()
    target.publish(event_id="one", logical_partition="a", sequence=1, tenant="tenant-a")
    delivery = target.poll(limit=1)[0]
    target.acknowledge(delivery.token)
    target.recover()
    assert target.poll(limit=1) == ()


@pytest.mark.lifecycle
def test_tenant_policy_denial_does_not_append() -> None:
    target = InMemoryStreamingTarget()
    with pytest.raises(StreamingPolicyDenied):
        target.publish(
            event_id="denied",
            logical_partition="a",
            sequence=1,
            tenant="tenant-b",
            authorized=False,
        )
    assert target.poll() == ()
