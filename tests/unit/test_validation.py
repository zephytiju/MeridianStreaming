# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage import ResourceRef
from meridian_storage.streaming import (
    MAX_BATCH_SIZE,
    Cursor,
    InvalidCursor,
    InvalidEvent,
    InvalidStreamingDefinition,
    validate_expression_arguments,
)


@pytest.mark.parametrize(
    ("method", "arguments"),
    [
        (
            "publish_schema",
            {
                "namespace": "orders",
                "name": "event",
                "version": "1.0.0",
                "definition": {},
                "allowBreaking": "no",
            },
        ),
        (
            "publish_schema",
            {
                "namespace": "orders",
                "name": "event",
                "version": "1.0.0",
                "definition": {},
                "allowBreaking": False,
                "expectedRegistryRevision": -1,
            },
        ),
        (
            "create_resource",
            {
                "namespace": "orders",
                "name": "events",
                "resourceType": "unknown",
                "schema": None,
                "options": {},
            },
        ),
        (
            "create_resource",
            {
                "namespace": "orders",
                "name": "events",
                "resourceType": "stream",
                "schema": [],
                "options": {},
            },
        ),
        ("publish", {"resource": 3, "data": {}}),
        ("publish", {"resource": "structured:orders.events", "data": {}}),
        ("publish", {"resource": "orders.events", "data": [], "idempotencyKey": "one"}),
        ("publish", {"resource": "orders.events", "data": {}, "idempotencyKey": 4}),
        ("publish_batch", {"resource": "orders.events", "data": "not-an-array"}),
        (
            "publish_batch",
            {"resource": "orders.events", "data": ({},) * (MAX_BATCH_SIZE + 1)},
        ),
        (
            "subscribe",
            {"stream": "orders.events", "subscription": "orders.sub", "options": []},
        ),
        (
            "poll",
            {
                "subscription": "orders.sub",
                "consumerGroup": "orders.group",
                "limit": 1,
                "waitTimeoutMs": -1,
            },
        ),
        (
            "acknowledge",
            {"subscription": "orders.sub", "consumerGroup": "orders.group", "delivery": 3},
        ),
        (
            "negative_acknowledge",
            {
                "subscription": "orders.sub",
                "consumerGroup": "orders.group",
                "delivery": "opaque",
                "retryAfterMs": -1,
            },
        ),
        (
            "negative_acknowledge",
            {
                "subscription": "orders.sub",
                "consumerGroup": "orders.group",
                "delivery": "opaque",
                "retryAfterMs": 0,
                "classification": 3,
            },
        ),
        (
            "read_range",
            {
                "resource": "orders.events",
                "start": 3,
                "end": None,
                "cursor": None,
                "limit": 1,
            },
        ),
        (
            "read_range",
            {
                "resource": "orders.events",
                "start": None,
                "end": None,
                "cursor": 3,
                "limit": 1,
            },
        ),
        ("unsupported", {}),
    ],
)
def test_invalid_expression_arguments_are_normalized(
    method: str,
    arguments: dict[str, object],
) -> None:
    with pytest.raises((InvalidStreamingDefinition, InvalidCursor)):
        validate_expression_arguments(method, arguments)


def test_invalid_event_content_preserves_specialized_failure() -> None:
    with pytest.raises(InvalidEvent):
        validate_expression_arguments(
            "publish",
            {"resource": "orders.events", "data": {"credential": "private"}},
        )


def test_read_range_cursor_envelope_and_resource_are_validated() -> None:
    other = Cursor("opaque", ResourceRef("streaming", "orders", "other"))
    arguments = {
        "resource": "orders.events",
        "start": None,
        "end": None,
        "cursor": other.to_dict(),
        "limit": 1,
    }
    with pytest.raises(InvalidCursor, match="another Resource"):
        validate_expression_arguments("read_range", arguments)
    arguments["cursor"] = {"decoded": "position"}
    with pytest.raises(InvalidCursor, match="invalid"):
        validate_expression_arguments("read_range", arguments)


def test_mapping_delivery_and_string_cursor_are_valid() -> None:
    validate_expression_arguments(
        "acknowledge",
        {
            "subscription": "orders.sub",
            "consumerGroup": "orders.group",
            "delivery": {"value": "opaque"},
        },
    )
    validate_expression_arguments(
        "read_range",
        {
            "resource": "orders.events",
            "start": None,
            "end": "opaque-end",
            "cursor": "opaque-cursor",
            "limit": 1,
        },
    )
