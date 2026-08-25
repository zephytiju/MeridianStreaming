# SPDX-License-Identifier: Apache-2.0
"""Explicit versioned replay and ConsumerGroup-position Operation contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from meridian_storage import Operation, ResourceRef
from meridian_storage.registry import CapabilityRequirement

from .canonical import JsonValue, bounded_string
from .data import Cursor, Position
from .resources import CATALOG_NAME
from .validation import MAX_RANGE_SIZE, parse_resource

REPLAY_OPERATION_CONTRACT = "meridian.streaming.replay"
GROUP_POSITION_OPERATION_CONTRACT = "meridian.streaming.group-position"
EXPLICIT_OPERATION_VERSION = "1.0.0"
_SHA256_FINGERPRINT_LENGTH = 71


def _opaque_mapping(value: Cursor | Position | str | Mapping[str, object]) -> JsonValue:
    if isinstance(value, Cursor | Position):
        return cast(JsonValue, value.to_dict())
    if isinstance(value, str):
        return bounded_string(value, "opaque position", 4096)
    if isinstance(value, Mapping):
        return cast(JsonValue, dict(value))
    raise TypeError("position must be opaque token or mapping Data")


@dataclass(frozen=True, slots=True)
class ReplayOperation:
    """Finite retained replay that never mutates ConsumerGroup position."""

    stream: ResourceRef | str | Mapping[str, object]
    start: Cursor | Position | str | Mapping[str, object]
    end: Cursor | Position | str | Mapping[str, object] | None = None
    limit: int = 100
    authorization_ref: str = "runtime-policy"
    reason: str = "explicit-replay"

    def to_operation(self) -> Operation:
        stream = parse_resource(self.stream, "stream")
        if isinstance(self.limit, bool) or not isinstance(self.limit, int):
            raise ValueError("replay limit must be an integer")
        if not 1 <= self.limit <= MAX_RANGE_SIZE:
            raise ValueError(f"replay limit must be between 1 and {MAX_RANGE_SIZE}")
        input_value: dict[str, JsonValue] = {
            "stream": cast(JsonValue, stream.to_dict()),
            "start": _opaque_mapping(self.start),
            "end": None if self.end is None else _opaque_mapping(self.end),
            "limit": self.limit,
            "authorizationRef": bounded_string(self.authorization_ref, "authorization_ref", 512),
            "reason": bounded_string(self.reason, "reason", 512),
            "mutatesConsumerGroup": False,
        }
        return Operation(
            catalog=CATALOG_NAME,
            operation_contract=REPLAY_OPERATION_CONTRACT,
            operation_version=EXPLICIT_OPERATION_VERSION,
            resources=(stream,),
            input=input_value,
            requirements=(
                CapabilityRequirement(
                    operation_contract=REPLAY_OPERATION_CONTRACT,
                    operation_version=EXPLICIT_OPERATION_VERSION,
                    guarantees=("explicit-replay", "finite-retained-range", "opaque-cursors"),
                    minimum_limits={"maxRangeSize": self.limit},
                ),
            ),
            read_only=True,
            idempotent=True,
        )


@dataclass(frozen=True, slots=True)
class GroupPositionTransition:
    """Audited compare-and-set transition of one logical ConsumerGroup position."""

    subscription: ResourceRef | str | Mapping[str, object]
    consumer_group: ResourceRef | str | Mapping[str, object]
    position: Cursor | Position | str | Mapping[str, object]
    expected_position_fingerprint: str
    authorization_ref: str
    reason: str

    def to_operation(self) -> Operation:
        subscription = parse_resource(self.subscription, "subscription")
        consumer_group = parse_resource(self.consumer_group, "consumer_group")
        fingerprint = bounded_string(
            self.expected_position_fingerprint,
            "expected_position_fingerprint",
            _SHA256_FINGERPRINT_LENGTH,
        )
        if not fingerprint.startswith("sha256:") or len(fingerprint) != _SHA256_FINGERPRINT_LENGTH:
            raise ValueError("expected_position_fingerprint must be a SHA-256 fingerprint")
        resources = tuple(sorted({subscription, consumer_group}))
        return Operation(
            catalog=CATALOG_NAME,
            operation_contract=GROUP_POSITION_OPERATION_CONTRACT,
            operation_version=EXPLICIT_OPERATION_VERSION,
            resources=resources,
            input={
                "subscription": cast(JsonValue, subscription.to_dict()),
                "consumerGroup": cast(JsonValue, consumer_group.to_dict()),
                "position": _opaque_mapping(self.position),
                "expectedPositionFingerprint": fingerprint,
                "authorizationRef": bounded_string(
                    self.authorization_ref, "authorization_ref", 512
                ),
                "reason": bounded_string(self.reason, "reason", 512),
            },
            requirements=(
                CapabilityRequirement(
                    operation_contract=GROUP_POSITION_OPERATION_CONTRACT,
                    operation_version=EXPLICIT_OPERATION_VERSION,
                    guarantees=(
                        "consumer-groups",
                        "explicit-group-position",
                        "opaque-cursors",
                    ),
                ),
            ),
            read_only=False,
            idempotent=True,
        )


__all__ = [
    "EXPLICIT_OPERATION_VERSION",
    "GROUP_POSITION_OPERATION_CONTRACT",
    "REPLAY_OPERATION_CONTRACT",
    "GroupPositionTransition",
    "ReplayOperation",
]
