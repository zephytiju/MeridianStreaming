# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from meridian_storage import ResourceRef, SchemaRef
from meridian_storage.streaming import (
    Cursor,
    CursorExpired,
    Delivery,
    DeliveryGuarantee,
    DeliveryToken,
    Event,
    InvalidCursor,
    InvalidEvent,
    Position,
    PublishReceipt,
    RangePage,
)


def test_cursor_round_trip_and_expiry(stream_ref: ResourceRef) -> None:
    future = datetime.now(UTC) + timedelta(hours=1)
    cursor = Cursor("opaque", stream_ref, future)
    assert Cursor.from_mapping(cursor.to_dict()) == cursor
    cursor.validate_active(datetime.now(UTC))
    expired = Cursor("old", stream_ref, datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(CursorExpired) as caught:
        expired.validate_active()
    assert caught.value.code == "MERIDIAN_STREAMING_CURSOR_EXPIRED"


def test_cursor_rejects_unknown_fields(stream_ref: ResourceRef) -> None:
    mapping = Cursor("opaque", stream_ref).to_dict()
    mapping["decoded"] = 1
    with pytest.raises(InvalidCursor):
        Cursor.from_mapping(mapping)
    with pytest.raises(InvalidCursor):
        Cursor.from_mapping(
            {
                "formatVersion": "meridian.streaming.cursor.v1",
                "value": "opaque",
                "resource": "not-an-object",
                "expiresAt": None,
            }
        )


def test_event_and_delivery_serialization(
    event: Event,
    position: Position,
    delivery_token: DeliveryToken,
) -> None:
    delivery = Delivery(
        event,
        delivery_token,
        position,
        attempt=2,
        redelivered=True,
        received_at="2026-08-25T12:00:01Z",
        acknowledgement_deadline="2026-08-25T12:00:31Z",
    )
    mapping = delivery.to_dict()
    assert mapping["event"]["eventId"] == "event-1"  # type: ignore[index]
    assert mapping["delivery"]["value"] == "opaque-delivery"  # type: ignore[index]
    assert event.fingerprint.startswith("sha256:")
    assert Delivery.from_mapping(mapping) == delivery
    assert Event.from_mapping(event.to_dict()) == event
    assert Position.from_mapping(position.to_dict()) == position
    assert DeliveryToken.from_mapping(delivery_token.to_dict()) == delivery_token


def test_delivery_invariants(
    event: Event,
    position: Position,
    delivery_token: DeliveryToken,
) -> None:
    with pytest.raises(ValueError):
        Delivery(event, delivery_token, position, 1, True, "2026-08-25T12:00:00Z")
    other = Position("other", ResourceRef("streaming", "orders", "other"))
    with pytest.raises(ValueError):
        Delivery(event, delivery_token, other, 1, False, "2026-08-25T12:00:00Z")


def test_publish_receipt_and_range_page(
    event: Event,
    position: Position,
    stream_ref: ResourceRef,
) -> None:
    receipt = PublishReceipt("event-1", position, None, "2026-08-25T12:00:02Z")
    assert receipt.to_dict()["effectiveGuarantee"] == "at-least-once"
    page = RangePage((event,), Cursor("next", stream_ref), False)
    assert page.to_dict()["truncated"] is False
    assert PublishReceipt.from_mapping(receipt.to_dict()) == receipt
    assert RangePage.from_mapping(page.to_dict()) == page
    with pytest.raises(ValueError):
        PublishReceipt("event-1", None, None, "2026-08-25T12:00:02Z")


def test_event_requires_matching_streaming_schema(stream_ref: ResourceRef) -> None:
    with pytest.raises(ValueError):
        Event(
            "event-1",
            stream_ref,
            SchemaRef("structured", "orders", "event", "1.0.0"),
            {},
            "2026-08-25T12:00:00Z",
            "2026-08-25T12:00:00Z",
        )


def test_mapping_parsers_reject_invalid_shapes(event: Event) -> None:
    mapping = event.to_dict()
    mapping["unknown"] = True
    with pytest.raises(Exception, match="unknown or missing"):
        Event.from_mapping(mapping)
    with pytest.raises(TypeError):
        RangePage.from_mapping({"events": ["bad"], "nextCursor": None, "truncated": False})


def test_range_page_requires_one_stream(event: Event) -> None:
    other = Event(
        "event-2",
        ResourceRef("streaming", "orders", "other"),
        SchemaRef("streaming", "orders", "event", "1.0.0"),
        {},
        "2026-08-25T12:00:00Z",
        "2026-08-25T12:00:00Z",
    )
    with pytest.raises(ValueError):
        RangePage((event, other), None)


def test_cursor_validates_format_resource_expiry_and_fingerprint(stream_ref: ResourceRef) -> None:
    cursor = Cursor("opaque", stream_ref)
    assert not cursor.is_expired()
    assert cursor.fingerprint.startswith("sha256:")
    with pytest.raises(InvalidCursor, match="format_version"):
        Cursor("opaque", stream_ref, format_version="future")
    with pytest.raises(InvalidCursor, match="streaming Resource"):
        Cursor("opaque", ResourceRef("structured", "orders", "events"))
    with pytest.raises(InvalidCursor, match="offset"):
        Cursor("opaque", stream_ref, "2026-08-25T12:00:00")


def test_position_and_delivery_token_validation(
    stream_ref: ResourceRef,
    subscription_ref: ResourceRef,
    group_ref: ResourceRef,
) -> None:
    position = Position("opaque", stream_ref)
    token = DeliveryToken(
        "opaque",
        subscription_ref,
        group_ref,
        "2026-08-25T13:00:00Z",
    )
    assert position.fingerprint.startswith("sha256:")
    assert token.fingerprint.startswith("sha256:")
    with pytest.raises(ValueError, match="format_version"):
        Position("opaque", stream_ref, format_version="future")
    with pytest.raises(ValueError, match="format_version"):
        DeliveryToken("opaque", subscription_ref, group_ref, format_version="future")
    with pytest.raises(ValueError, match="unknown or missing"):
        Position.from_mapping({"value": "opaque"})
    with pytest.raises(TypeError, match="object"):
        Position.from_mapping(
            {
                "formatVersion": "meridian.streaming.position.v1",
                "value": "opaque",
                "resource": "orders.events",
                "logicalPartition": None,
            }
        )
    with pytest.raises(ValueError, match="unknown or missing"):
        DeliveryToken.from_mapping({"value": "opaque"})
    with pytest.raises(TypeError, match="objects"):
        DeliveryToken.from_mapping(
            {
                "formatVersion": "meridian.streaming.delivery-token.v1",
                "value": "opaque",
                "subscription": "orders.sub",
                "consumerGroup": "orders.group",
                "expiresAt": None,
            }
        )


def test_event_rejects_invalid_envelopes(stream_ref: ResourceRef, schema_ref: SchemaRef) -> None:
    arguments = (
        "event-1",
        stream_ref,
        schema_ref,
        {},
        "2026-08-25T12:00:00Z",
        "2026-08-25T12:00:00Z",
    )
    with pytest.raises(ValueError, match="format_version"):
        Event(*arguments, format_version="future")
    with pytest.raises(TypeError, match="objects"):
        Event(*arguments[:3], [], *arguments[4:])  # type: ignore[arg-type]
    mapping = Event(*arguments).to_dict()
    mapping["data"] = []
    with pytest.raises(InvalidEvent, match="mappings"):
        Event.from_mapping(mapping)
    mapping = Event(*arguments).to_dict()
    mapping["eventId"] = "has space"
    with pytest.raises(InvalidEvent, match="invalid"):
        Event.from_mapping(mapping)


def test_delivery_and_result_parser_shape_failures(
    event: Event,
    position: Position,
    delivery_token: DeliveryToken,
    stream_ref: ResourceRef,
) -> None:
    with pytest.raises(ValueError, match="format_version"):
        Delivery(
            event,
            delivery_token,
            position,
            1,
            False,
            "2026-08-25T12:00:00Z",
            format_version="future",
        )
    with pytest.raises(ValueError, match="positive"):
        Delivery(event, delivery_token, position, 0, False, "2026-08-25T12:00:00Z")
    with pytest.raises(TypeError, match="boolean"):
        Delivery(event, delivery_token, position, 1, 1, "2026-08-25T12:00:00Z")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown or missing"):
        Delivery.from_mapping({"event": {}})
    mapping = Delivery(
        event,
        delivery_token,
        position,
        1,
        False,
        "2026-08-25T12:00:00Z",
    ).to_dict()
    mapping["position"] = "opaque"
    with pytest.raises(TypeError, match="objects"):
        Delivery.from_mapping(mapping)

    cursor_receipt = PublishReceipt(
        "event-1",
        None,
        Cursor("accepted", stream_ref),
        "2026-08-25T12:00:00Z",
    )
    assert PublishReceipt.from_mapping(cursor_receipt.to_dict()) == cursor_receipt
    with pytest.raises(ValueError, match="at-least-once"):
        PublishReceipt(
            "event-1",
            position,
            None,
            "2026-08-25T12:00:00Z",
            effective_guarantee="other",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="unknown or missing"):
        PublishReceipt.from_mapping({"eventId": "event-1"})
    with pytest.raises(TypeError, match="position"):
        PublishReceipt.from_mapping(
            {
                "eventId": "event-1",
                "position": "opaque",
                "cursor": None,
                "acceptedAt": "2026-08-25T12:00:00Z",
                "effectiveGuarantee": DeliveryGuarantee.AT_LEAST_ONCE.value,
            }
        )


def test_range_page_parser_and_cursor_invariants(event: Event) -> None:
    with pytest.raises(TypeError, match="boolean"):
        RangePage((event,), None, truncated=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="target"):
        RangePage(
            (event,),
            Cursor("next", ResourceRef("streaming", "orders", "other")),
        )
    with pytest.raises(ValueError, match="unknown or missing"):
        RangePage.from_mapping({"events": []})
    with pytest.raises(TypeError, match="array"):
        RangePage.from_mapping({"events": {}, "nextCursor": None, "truncated": False})
    with pytest.raises(TypeError, match="object or null"):
        RangePage.from_mapping({"events": [], "nextCursor": "opaque", "truncated": False})
