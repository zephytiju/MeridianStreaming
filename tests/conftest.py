# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from meridian_storage import ResourceRef, SchemaRef
from meridian_storage.streaming import DeliveryToken, Event, Position


@pytest.fixture
def stream_ref() -> ResourceRef:
    return ResourceRef("streaming", "orders", "events")


@pytest.fixture
def subscription_ref() -> ResourceRef:
    return ResourceRef("streaming", "orders", "subscription")


@pytest.fixture
def group_ref() -> ResourceRef:
    return ResourceRef("streaming", "orders", "workers")


@pytest.fixture
def schema_ref() -> SchemaRef:
    return SchemaRef("streaming", "orders", "event", "1.0.0")


@pytest.fixture
def event(stream_ref: ResourceRef, schema_ref: SchemaRef) -> Event:
    return Event(
        "event-1",
        stream_ref,
        schema_ref,
        {"order_id": "order-1", "value": 3},
        datetime(2026, 8, 25, 12, tzinfo=UTC),
        "2026-08-25T12:00:00.000001Z",
        logical_partition_key="order-1",
        headers={"content-type": "application/json"},
        trace_context={"trace-id": "trace-1"},
    )


@pytest.fixture
def position(stream_ref: ResourceRef) -> Position:
    return Position("opaque-position", stream_ref, "partition-a")


@pytest.fixture
def delivery_token(subscription_ref: ResourceRef, group_ref: ResourceRef) -> DeliveryToken:
    return DeliveryToken("opaque-delivery", subscription_ref, group_ref)
