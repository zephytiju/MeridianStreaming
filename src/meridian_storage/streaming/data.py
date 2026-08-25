# SPDX-License-Identifier: Apache-2.0
"""Event, delivery, position, and opaque Cursor Data contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from meridian_storage import ResourceRef, SchemaRef

from .canonical import (
    JsonValue,
    bounded_string,
    contract_token,
    freeze_json,
    sha256_fingerprint,
    string_map,
    thaw_json,
    utc_timestamp,
)
from .errors import CursorExpired, InvalidCursor, InvalidEvent
from .resources import CATALOG_NAME, DeliveryGuarantee

CURSOR_FORMAT_VERSION = "meridian.streaming.cursor.v1"
POSITION_FORMAT_VERSION = "meridian.streaming.position.v1"
# This constant is a public serialization identifier, not authentication material.
DELIVERY_TOKEN_FORMAT_VERSION = (  # nosec B105
    "meridian.streaming.delivery-token.v1"
)
EVENT_FORMAT_VERSION = "meridian.streaming.event.v1"
DELIVERY_FORMAT_VERSION = "meridian.streaming.delivery.v1"


def _streaming_ref(value: ResourceRef | str | Mapping[str, object], field_name: str) -> ResourceRef:
    try:
        resource = ResourceRef.parse(value, catalog=CATALOG_NAME)
        if resource.catalog != CATALOG_NAME:
            raise ValueError("Resource belongs to another Catalog")
        return resource
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a streaming Resource reference") from exc


def _schema_ref(value: SchemaRef | Mapping[str, object]) -> SchemaRef:
    schema = SchemaRef.parse(value)
    if schema.catalog != CATALOG_NAME:
        raise ValueError("Event Schema must belong to the streaming Catalog")
    return schema


def _opaque(value: str, field_name: str) -> str:
    return bounded_string(value, field_name, 4096)


@dataclass(frozen=True, slots=True)
class Cursor:
    """Opaque retained-range coordinate; consumers must never decode ``value``."""

    value: str
    resource: ResourceRef
    expires_at: str | datetime | None = None
    format_version: str = CURSOR_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != CURSOR_FORMAT_VERSION:
            raise InvalidCursor(f"format_version must be {CURSOR_FORMAT_VERSION!r}")
        object.__setattr__(self, "value", _opaque(self.value, "Cursor value"))
        try:
            object.__setattr__(self, "resource", _streaming_ref(self.resource, "Cursor resource"))
            if self.expires_at is not None:
                object.__setattr__(self, "expires_at", utc_timestamp(self.expires_at, "expires_at"))
        except (TypeError, ValueError) as exc:
            raise InvalidCursor(str(exc), requirement="cursor.envelope") from exc

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def is_expired(self, at: datetime | None = None) -> bool:
        if self.expires_at is None:
            return False
        selected = (at or datetime.now(UTC)).astimezone(UTC)
        expiry = datetime.fromisoformat(cast(str, self.expires_at).replace("Z", "+00:00"))
        return selected >= expiry

    def validate_active(self, at: datetime | None = None) -> None:
        if self.is_expired(at):
            raise CursorExpired(
                resource_ref=str(self.resource),
                requirement="cursor.retained-range",
                logical_references=(str(self.resource),),
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "value": self.value,
            "resource": self.resource.to_dict(),
            "expiresAt": self.expires_at,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Cursor:
        expected = {"formatVersion", "value", "resource", "expiresAt"}
        if set(value) != expected:
            raise InvalidCursor("Cursor contains unknown or missing fields")
        try:
            resource = value["resource"]
            if not isinstance(resource, Mapping):
                raise TypeError("Cursor resource must be an object")
            return cls(
                format_version=cast(str, value["formatVersion"]),
                value=cast(str, value["value"]),
                resource=ResourceRef.parse(cast(Mapping[str, object], resource)),
                expires_at=cast(str | None, value["expiresAt"]),
            )
        except InvalidCursor:
            raise
        except (TypeError, ValueError) as exc:
            raise InvalidCursor("Cursor envelope is invalid") from exc


@dataclass(frozen=True, slots=True)
class Position:
    """Opaque position with an optional public logical-partition label."""

    value: str
    resource: ResourceRef
    logical_partition: str | None = None
    format_version: str = POSITION_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != POSITION_FORMAT_VERSION:
            raise ValueError(f"format_version must be {POSITION_FORMAT_VERSION!r}")
        object.__setattr__(self, "value", _opaque(self.value, "Position value"))
        object.__setattr__(self, "resource", _streaming_ref(self.resource, "Position resource"))
        if self.logical_partition is not None:
            object.__setattr__(
                self,
                "logical_partition",
                bounded_string(self.logical_partition, "logical partition", 512),
            )

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "value": self.value,
            "resource": self.resource.to_dict(),
            "logicalPartition": self.logical_partition,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Position:
        expected = {"formatVersion", "value", "resource", "logicalPartition"}
        if set(value) != expected:
            raise ValueError("Position contains unknown or missing fields")
        resource = value["resource"]
        if not isinstance(resource, Mapping):
            raise TypeError("Position resource must be an object")
        return cls(
            format_version=cast(str, value["formatVersion"]),
            value=cast(str, value["value"]),
            resource=ResourceRef.parse(cast(Mapping[str, object], resource)),
            logical_partition=cast(str | None, value["logicalPartition"]),
        )


@dataclass(frozen=True, slots=True)
class DeliveryToken:
    """Opaque acknowledgement token scoped to Subscription and ConsumerGroup."""

    value: str
    subscription: ResourceRef
    consumer_group: ResourceRef
    expires_at: str | datetime | None = None
    format_version: str = DELIVERY_TOKEN_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != DELIVERY_TOKEN_FORMAT_VERSION:
            raise ValueError(f"format_version must be {DELIVERY_TOKEN_FORMAT_VERSION!r}")
        object.__setattr__(self, "value", _opaque(self.value, "delivery token"))
        object.__setattr__(
            self,
            "subscription",
            _streaming_ref(self.subscription, "delivery subscription"),
        )
        object.__setattr__(
            self,
            "consumer_group",
            _streaming_ref(self.consumer_group, "delivery consumer group"),
        )
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", utc_timestamp(self.expires_at, "expires_at"))

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "value": self.value,
            "subscription": self.subscription.to_dict(),
            "consumerGroup": self.consumer_group.to_dict(),
            "expiresAt": self.expires_at,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> DeliveryToken:
        expected = {
            "formatVersion",
            "value",
            "subscription",
            "consumerGroup",
            "expiresAt",
        }
        if set(value) != expected:
            raise ValueError("DeliveryToken contains unknown or missing fields")
        subscription = value["subscription"]
        consumer_group = value["consumerGroup"]
        if not isinstance(subscription, Mapping) or not isinstance(consumer_group, Mapping):
            raise TypeError("DeliveryToken Resource references must be objects")
        return cls(
            format_version=cast(str, value["formatVersion"]),
            value=cast(str, value["value"]),
            subscription=ResourceRef.parse(cast(Mapping[str, object], subscription)),
            consumer_group=ResourceRef.parse(cast(Mapping[str, object], consumer_group)),
            expires_at=cast(str | None, value["expiresAt"]),
        )


@dataclass(frozen=True, slots=True)
class Event:
    """Schema-governed Event carried by provider-neutral Operations."""

    event_id: str
    stream: ResourceRef
    schema: SchemaRef
    data: Mapping[str, JsonValue]
    occurred_at: str | datetime
    produced_at: str | datetime
    logical_partition_key: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    trace_context: Mapping[str, str] = field(default_factory=dict)
    extensions: Mapping[str, JsonValue] = field(default_factory=dict)
    format_version: str = EVENT_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != EVENT_FORMAT_VERSION:
            raise ValueError(f"format_version must be {EVENT_FORMAT_VERSION!r}")
        object.__setattr__(self, "event_id", contract_token(self.event_id, "event_id"))
        stream = _streaming_ref(self.stream, "Event stream")
        schema = _schema_ref(self.schema)
        if schema.namespace != stream.namespace:
            raise ValueError("Event Stream and Schema must share a Namespace")
        object.__setattr__(self, "stream", stream)
        object.__setattr__(self, "schema", schema)
        frozen_data = freeze_json(self.data, path="$.data")
        frozen_extensions = freeze_json(self.extensions, path="$.extensions")
        if not isinstance(frozen_data, Mapping) or not isinstance(frozen_extensions, Mapping):
            raise TypeError("Event data and extensions must be objects")
        object.__setattr__(self, "data", frozen_data)
        object.__setattr__(self, "extensions", frozen_extensions)
        object.__setattr__(self, "occurred_at", utc_timestamp(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "produced_at", utc_timestamp(self.produced_at, "produced_at"))
        if self.logical_partition_key is not None:
            object.__setattr__(
                self,
                "logical_partition_key",
                bounded_string(self.logical_partition_key, "logical_partition_key", 1024),
            )
        object.__setattr__(self, "headers", string_map(self.headers, "headers"))
        object.__setattr__(self, "trace_context", string_map(self.trace_context, "trace_context"))

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "eventId": self.event_id,
            "stream": self.stream.to_dict(),
            "schema": self.schema.to_dict(),
            "data": thaw_json(cast(JsonValue, self.data)),
            "occurredAt": self.occurred_at,
            "producedAt": self.produced_at,
            "logicalPartitionKey": self.logical_partition_key,
            "headers": dict(self.headers),
            "traceContext": dict(self.trace_context),
            "extensions": thaw_json(cast(JsonValue, self.extensions)),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Event:
        expected = {
            "formatVersion",
            "eventId",
            "stream",
            "schema",
            "data",
            "occurredAt",
            "producedAt",
            "logicalPartitionKey",
            "headers",
            "traceContext",
            "extensions",
        }
        if set(value) != expected:
            raise InvalidEvent("Event contains unknown or missing fields")
        stream = value["stream"]
        schema = value["schema"]
        data = value["data"]
        headers = value["headers"]
        trace_context = value["traceContext"]
        extensions = value["extensions"]
        if not all(
            isinstance(item, Mapping)
            for item in (stream, schema, data, headers, trace_context, extensions)
        ):
            raise InvalidEvent("Event object fields must be mappings")
        try:
            return cls(
                format_version=cast(str, value["formatVersion"]),
                event_id=cast(str, value["eventId"]),
                stream=ResourceRef.parse(cast(Mapping[str, object], stream)),
                schema=SchemaRef.parse(cast(Mapping[str, object], schema)),
                data=cast(Mapping[str, JsonValue], data),
                occurred_at=cast(str, value["occurredAt"]),
                produced_at=cast(str, value["producedAt"]),
                logical_partition_key=cast(str | None, value["logicalPartitionKey"]),
                headers=cast(Mapping[str, str], headers),
                trace_context=cast(Mapping[str, str], trace_context),
                extensions=cast(Mapping[str, JsonValue], extensions),
            )
        except InvalidEvent:
            raise
        except (TypeError, ValueError) as exc:
            raise InvalidEvent("Event envelope is invalid") from exc


@dataclass(frozen=True, slots=True)
class Delivery:
    event: Event
    delivery: DeliveryToken
    position: Position
    attempt: int
    redelivered: bool
    received_at: str | datetime
    acknowledgement_deadline: str | datetime | None = None
    format_version: str = DELIVERY_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != DELIVERY_FORMAT_VERSION:
            raise ValueError(f"format_version must be {DELIVERY_FORMAT_VERSION!r}")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("attempt must be a positive integer")
        if not isinstance(self.redelivered, bool):
            raise TypeError("redelivered must be boolean")
        if self.redelivered != (self.attempt > 1):
            raise ValueError("redelivered must be true exactly when attempt is greater than one")
        if self.position.resource != self.event.stream:
            raise ValueError("Delivery position must target the Event Stream")
        object.__setattr__(self, "received_at", utc_timestamp(self.received_at, "received_at"))
        if self.acknowledgement_deadline is not None:
            object.__setattr__(
                self,
                "acknowledgement_deadline",
                utc_timestamp(self.acknowledgement_deadline, "acknowledgement_deadline"),
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "event": self.event.to_dict(),
            "delivery": self.delivery.to_dict(),
            "position": self.position.to_dict(),
            "attempt": self.attempt,
            "redelivered": self.redelivered,
            "receivedAt": self.received_at,
            "acknowledgementDeadline": self.acknowledgement_deadline,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Delivery:
        expected = {
            "formatVersion",
            "event",
            "delivery",
            "position",
            "attempt",
            "redelivered",
            "receivedAt",
            "acknowledgementDeadline",
        }
        if set(value) != expected:
            raise ValueError("Delivery contains unknown or missing fields")
        event = value["event"]
        token = value["delivery"]
        position = value["position"]
        if not all(isinstance(item, Mapping) for item in (event, token, position)):
            raise TypeError("Delivery Event, token, and Position must be objects")
        return cls(
            format_version=cast(str, value["formatVersion"]),
            event=Event.from_mapping(cast(Mapping[str, object], event)),
            delivery=DeliveryToken.from_mapping(cast(Mapping[str, object], token)),
            position=Position.from_mapping(cast(Mapping[str, object], position)),
            attempt=cast(int, value["attempt"]),
            redelivered=cast(bool, value["redelivered"]),
            received_at=cast(str, value["receivedAt"]),
            acknowledgement_deadline=cast(str | None, value["acknowledgementDeadline"]),
        )


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    event_id: str
    position: Position | None
    cursor: Cursor | None
    accepted_at: str | datetime
    effective_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", contract_token(self.event_id, "event_id"))
        if self.position is None and self.cursor is None:
            raise ValueError("PublishReceipt requires an accepted Position or Cursor")
        object.__setattr__(self, "accepted_at", utc_timestamp(self.accepted_at, "accepted_at"))
        if self.effective_guarantee is not DeliveryGuarantee.AT_LEAST_ONCE:
            raise ValueError("V1 receipt guarantee must be at-least-once")

    def to_dict(self) -> dict[str, object]:
        return {
            "eventId": self.event_id,
            "position": None if self.position is None else self.position.to_dict(),
            "cursor": None if self.cursor is None else self.cursor.to_dict(),
            "acceptedAt": self.accepted_at,
            "effectiveGuarantee": self.effective_guarantee.value,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> PublishReceipt:
        expected = {"eventId", "position", "cursor", "acceptedAt", "effectiveGuarantee"}
        if set(value) != expected:
            raise ValueError("PublishReceipt contains unknown or missing fields")
        raw_position = value["position"]
        raw_cursor = value["cursor"]
        if raw_position is not None and not isinstance(raw_position, Mapping):
            raise TypeError("PublishReceipt position must be an object or null")
        if raw_cursor is not None and not isinstance(raw_cursor, Mapping):
            raise TypeError("PublishReceipt cursor must be an object or null")
        return cls(
            event_id=cast(str, value["eventId"]),
            position=(
                None
                if raw_position is None
                else Position.from_mapping(cast(Mapping[str, object], raw_position))
            ),
            cursor=(
                None
                if raw_cursor is None
                else Cursor.from_mapping(cast(Mapping[str, object], raw_cursor))
            ),
            accepted_at=cast(str, value["acceptedAt"]),
            effective_guarantee=DeliveryGuarantee(cast(str, value["effectiveGuarantee"])),
        )


@dataclass(frozen=True, slots=True)
class RangePage:
    events: tuple[Event, ...]
    next_cursor: Cursor | None
    truncated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be boolean")
        if (
            self.next_cursor is not None
            and self.events
            and self.next_cursor.resource != self.events[0].stream
        ):
            raise ValueError("Range Cursor must target the Event Stream")
        if self.events and any(event.stream != self.events[0].stream for event in self.events):
            raise ValueError("one RangePage cannot contain Events from different Streams")

    def to_dict(self) -> dict[str, object]:
        return {
            "events": [event.to_dict() for event in self.events],
            "nextCursor": None if self.next_cursor is None else self.next_cursor.to_dict(),
            "truncated": self.truncated,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> RangePage:
        expected = {"events", "nextCursor", "truncated"}
        if set(value) != expected:
            raise ValueError("RangePage contains unknown or missing fields")
        raw_events = value["events"]
        raw_cursor = value["nextCursor"]
        if not isinstance(raw_events, tuple | list):
            raise TypeError("RangePage events must be an array")
        if not all(isinstance(item, Mapping) for item in raw_events):
            raise TypeError("RangePage events must contain Event objects")
        if raw_cursor is not None and not isinstance(raw_cursor, Mapping):
            raise TypeError("RangePage nextCursor must be an object or null")
        return cls(
            events=tuple(
                Event.from_mapping(cast(Mapping[str, object], item)) for item in raw_events
            ),
            next_cursor=(
                None
                if raw_cursor is None
                else Cursor.from_mapping(cast(Mapping[str, object], raw_cursor))
            ),
            truncated=cast(bool, value["truncated"]),
        )


__all__ = [
    "CURSOR_FORMAT_VERSION",
    "DELIVERY_FORMAT_VERSION",
    "DELIVERY_TOKEN_FORMAT_VERSION",
    "EVENT_FORMAT_VERSION",
    "POSITION_FORMAT_VERSION",
    "Cursor",
    "Delivery",
    "DeliveryToken",
    "Event",
    "Position",
    "PublishReceipt",
    "RangePage",
]
