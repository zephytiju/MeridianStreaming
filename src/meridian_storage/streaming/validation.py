# SPDX-License-Identifier: Apache-2.0
"""Mapping-first validation for Streaming Expressions and explicit Operations."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import cast

from meridian_storage import ResourceRef

from .canonical import JsonValue, bounded_string, freeze_json
from .data import Cursor
from .errors import InvalidCursor, InvalidEvent, InvalidStreamingDefinition
from .resources import CATALOG_NAME, ResourceKind

MAX_BATCH_SIZE = 10_000
MAX_POLL_SIZE = 10_000
MAX_RANGE_SIZE = 10_000
MAX_WAIT_TIMEOUT_MS = 300_000


def parse_resource(value: object, field_name: str) -> ResourceRef:
    try:
        if isinstance(value, ResourceRef | str | Mapping):
            resource = ResourceRef.parse(value, catalog=CATALOG_NAME)
            if resource.catalog != CATALOG_NAME:
                raise ValueError("Resource belongs to another Catalog")
            return resource
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(
            f"{field_name} is not a valid logical streaming Resource reference",
            requirement="operation.resource",
        ) from exc
    raise InvalidStreamingDefinition(
        f"{field_name} requires a logical streaming Resource reference",
        requirement="operation.resource",
    )


def _shape(
    method: str,
    value: Mapping[str, object],
    required: set[str],
    optional: set[str] | frozenset[str] = frozenset(),
) -> None:
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing or unknown:
        raise InvalidStreamingDefinition(
            f"streaming.{method} contains unknown or missing arguments",
            requirement="expression.arguments",
        )


def _positive_int(value: object, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise InvalidStreamingDefinition(
            f"{field_name} must be between 1 and {maximum}",
            requirement=f"operation.limit.{field_name}",
        )
    return value


def _nonnegative_int(value: object, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise InvalidStreamingDefinition(
            f"{field_name} must be between 0 and {maximum}",
            requirement=f"operation.limit.{field_name}",
        )
    return value


def _mapping(value: object, field_name: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise InvalidStreamingDefinition(
            f"{field_name} must be an object",
            requirement="expression.arguments",
        )
    try:
        frozen = freeze_json(value, path=f"$.{field_name}")
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(
            f"{field_name} must contain portable JSON Data",
            requirement="expression.arguments",
        ) from exc
    return cast(Mapping[str, JsonValue], frozen)


def _event(value: object, field_name: str = "data") -> Mapping[str, JsonValue]:
    event = _mapping(value, field_name)
    forbidden = {
        "engine",
        "endpoint",
        "credential",
        "physicalName",
        "binding",
        "adapter",
    }
    leaked = forbidden & set(event)
    if leaked:
        raise InvalidEvent(
            "Event envelope contains deployment-private fields",
            requirement="event.provider-neutral",
        )
    return event


def validate_expression_arguments(method: str, value: Mapping[str, object]) -> None:
    """Validate one exact public Expression argument shape."""

    try:
        _validate_expression_arguments(method, value)
    except (InvalidStreamingDefinition, InvalidEvent, InvalidCursor):
        raise
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(
            f"streaming.{method} contains invalid arguments",
            requirement="expression.arguments",
        ) from exc


def _validate_publish_schema(method: str, value: Mapping[str, object]) -> None:
    _shape(
        method,
        value,
        {"namespace", "name", "version", "definition", "allowBreaking"},
        {"expectedRegistryRevision"},
    )
    for field_name in ("namespace", "name", "version"):
        bounded_string(cast(str, value[field_name]), field_name)
    _mapping(value["definition"], "definition")
    if not isinstance(value["allowBreaking"], bool):
        raise InvalidStreamingDefinition("allowBreaking must be boolean")
    expected = value.get("expectedRegistryRevision")
    if expected is not None:
        _nonnegative_int(expected, "expectedRegistryRevision", 2**63 - 1)


def _validate_create_resource(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"namespace", "name", "resourceType", "schema", "options"})
    for field_name in ("namespace", "name"):
        bounded_string(cast(str, value[field_name]), field_name)
    try:
        ResourceKind(cast(str, value["resourceType"]))
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(
            "resourceType is not a V1 Streaming Resource kind"
        ) from exc
    schema = value["schema"]
    if schema is not None:
        _mapping(schema, "schema")
    _mapping(value["options"], "options")


def _validate_idempotency_key(value: Mapping[str, object]) -> None:
    idempotency_key = value.get("idempotencyKey")
    if idempotency_key is not None:
        bounded_string(cast(str, idempotency_key), "idempotencyKey", 512)


def _validate_publish(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"resource", "data"}, {"idempotencyKey"})
    parse_resource(value["resource"], "resource")
    _event(value["data"])
    _validate_idempotency_key(value)


def _validate_publish_batch(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"resource", "data"}, {"idempotencyKey"})
    parse_resource(value["resource"], "resource")
    data = value["data"]
    if not isinstance(data, Sequence) or isinstance(data, str | bytes):
        raise InvalidStreamingDefinition("publish_batch data must be an array")
    if not data or len(data) > MAX_BATCH_SIZE:
        raise InvalidStreamingDefinition(
            f"publish_batch data must contain between 1 and {MAX_BATCH_SIZE} Events",
            requirement="operation.limit.maxBatchSize",
        )
    for item in data:
        _event(item)
    _validate_idempotency_key(value)


def _validate_subscribe(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"stream", "subscription", "options"})
    parse_resource(value["stream"], "stream")
    parse_resource(value["subscription"], "subscription")
    _mapping(value["options"], "options")


def _validate_poll(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"subscription", "consumerGroup", "limit", "waitTimeoutMs"})
    parse_resource(value["subscription"], "subscription")
    parse_resource(value["consumerGroup"], "consumerGroup")
    _positive_int(value["limit"], "limit", MAX_POLL_SIZE)
    _nonnegative_int(value["waitTimeoutMs"], "waitTimeoutMs", MAX_WAIT_TIMEOUT_MS)


def _validate_delivery(value: object) -> None:
    if not isinstance(value, str | Mapping):
        raise InvalidStreamingDefinition("delivery must be an opaque token or mapping")
    if isinstance(value, str):
        bounded_string(value, "delivery", 4096)
    else:
        _mapping(value, "delivery")


def _validate_acknowledge(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"subscription", "consumerGroup", "delivery"})
    parse_resource(value["subscription"], "subscription")
    parse_resource(value["consumerGroup"], "consumerGroup")
    _validate_delivery(value["delivery"])


def _validate_negative_acknowledge(method: str, value: Mapping[str, object]) -> None:
    _shape(
        method,
        value,
        {"subscription", "consumerGroup", "delivery"},
        {"retryAfterMs", "classification"},
    )
    parse_resource(value["subscription"], "subscription")
    parse_resource(value["consumerGroup"], "consumerGroup")
    _validate_delivery(value["delivery"])
    _nonnegative_int(value.get("retryAfterMs", 0), "retryAfterMs", 86_400_000)
    classification = value.get("classification")
    if classification is not None:
        bounded_string(cast(str, classification), "classification", 256)


def _validate_read_range(method: str, value: Mapping[str, object]) -> None:
    _shape(method, value, {"resource", "start", "end", "cursor", "limit"})
    resource = parse_resource(value["resource"], "resource")
    start, cursor = value["start"], value["cursor"]
    if start is not None and cursor is not None:
        raise InvalidStreamingDefinition(
            "read_range accepts start or cursor, never both",
            requirement="range.bounds",
        )
    for item, field_name in ((start, "start"), (value["end"], "end")):
        if item is not None and not isinstance(item, str | Mapping):
            raise InvalidStreamingDefinition(f"{field_name} must be opaque Data")
    if isinstance(cursor, str):
        bounded_string(cursor, "cursor", 4096)
    elif isinstance(cursor, Mapping):
        try:
            parsed_cursor = Cursor.from_mapping(cursor)
        except (TypeError, ValueError, InvalidCursor) as exc:
            raise InvalidCursor("read_range Cursor is invalid") from exc
        if parsed_cursor.resource != resource:
            raise InvalidCursor("read_range Cursor targets another Resource")
    elif cursor is not None:
        raise InvalidCursor("cursor must be an opaque token or Cursor mapping")
    _positive_int(value["limit"], "limit", MAX_RANGE_SIZE)


type _Validator = Callable[[str, Mapping[str, object]], None]

_VALIDATORS: Mapping[str, _Validator] = {
    "acknowledge": _validate_acknowledge,
    "create_resource": _validate_create_resource,
    "negative_acknowledge": _validate_negative_acknowledge,
    "poll": _validate_poll,
    "publish": _validate_publish,
    "publish_batch": _validate_publish_batch,
    "publish_schema": _validate_publish_schema,
    "read_range": _validate_read_range,
    "subscribe": _validate_subscribe,
}


def _validate_expression_arguments(method: str, value: Mapping[str, object]) -> None:
    """Internal validator whose low-level failures are normalized by the public wrapper."""

    try:
        validator = _VALIDATORS[method]
    except KeyError as exc:
        raise InvalidStreamingDefinition(
            f"unsupported streaming Expression method {method!r}",
            requirement="expression.method",
        ) from exc
    validator(method, value)


__all__ = [
    "MAX_BATCH_SIZE",
    "MAX_POLL_SIZE",
    "MAX_RANGE_SIZE",
    "MAX_WAIT_TIMEOUT_MS",
    "parse_resource",
    "validate_expression_arguments",
]
