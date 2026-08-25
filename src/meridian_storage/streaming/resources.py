# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral Stream, Subscription, and ConsumerGroup resources."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

from meridian_storage import ResourceRef, SchemaRef

from .canonical import (
    JsonValue,
    bounded_string,
    contract_token,
    freeze_json,
    sha256_fingerprint,
    thaw_json,
)

CATALOG_NAME = "streaming"
RESOURCE_FORMAT_VERSION = "meridian.streaming.resource.v1"
MAX_LOGICAL_PARTITIONS = 1_000_000


class ResourceKind(StrEnum):
    STREAM = "stream"
    SUBSCRIPTION = "subscription"
    CONSUMER_GROUP = "consumer-group"


class DeliveryGuarantee(StrEnum):
    AT_LEAST_ONCE = "at-least-once"


class OrderingGuarantee(StrEnum):
    PER_LOGICAL_PARTITION = "per-logical-partition"


def _streaming_ref(value: ResourceRef | str | Mapping[str, object], field_name: str) -> ResourceRef:
    try:
        resource = ResourceRef.parse(value, catalog=CATALOG_NAME)
        if resource.catalog != CATALOG_NAME:
            raise ValueError("Resource belongs to another Catalog")
        return resource
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a streaming Resource reference") from exc


def _streaming_schema(value: SchemaRef | Mapping[str, object]) -> SchemaRef:
    try:
        schema = SchemaRef.parse(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("schema must be an exact streaming Schema reference") from exc
    if schema.catalog != CATALOG_NAME:
        raise ValueError("schema must belong to the streaming Catalog")
    return schema


def _scope(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(sorted(contract_token(value, "required scope") for value in values))
    if len(set(result)) != len(result):
        raise ValueError("required scope entries must be unique")
    return result


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    """Cross-cutting policy and evidence requirements, never identity implementation."""

    required_scope: tuple[str, ...] = ("tenant",)
    policy_ref: str | None = None
    audit_required: bool = True
    lineage_required: bool = True
    telemetry_required: bool = True
    extensions: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "required_scope", _scope(self.required_scope))
        if self.policy_ref is not None:
            object.__setattr__(
                self, "policy_ref", bounded_string(self.policy_ref, "policy_ref", 512)
            )
        for field_name in ("audit_required", "lineage_required", "telemetry_required"):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be boolean")
        frozen = freeze_json(self.extensions, path="$.extensions")
        if not isinstance(frozen, Mapping):
            raise TypeError("extensions must be an object")
        object.__setattr__(self, "extensions", frozen)

    def to_dict(self) -> dict[str, object]:
        return {
            "requiredScope": list(self.required_scope),
            "policyRef": self.policy_ref,
            "evidence": {
                "audit": self.audit_required,
                "lineage": self.lineage_required,
                "telemetry": self.telemetry_required,
            },
            "extensions": thaw_json(cast(JsonValue, self.extensions)),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object] | None) -> ResourcePolicy:
        if value is None:
            return cls()
        allowed = {"requiredScope", "policyRef", "evidence", "extensions"}
        if set(value) - allowed:
            raise ValueError("resource policy contains unknown fields")
        raw_scope = value.get("requiredScope", ("tenant",))
        evidence = value.get("evidence", {})
        extensions = value.get("extensions", {})
        if not isinstance(raw_scope, Sequence) or isinstance(raw_scope, str | bytes):
            raise TypeError("requiredScope must be an array")
        if not isinstance(evidence, Mapping) or not isinstance(extensions, Mapping):
            raise TypeError("evidence and extensions must be objects")
        if set(evidence) - {"audit", "lineage", "telemetry"}:
            raise ValueError("evidence contains unknown fields")
        return cls(
            required_scope=tuple(cast(str, item) for item in raw_scope),
            policy_ref=cast(str | None, value.get("policyRef")),
            audit_required=cast(bool, evidence.get("audit", True)),
            lineage_required=cast(bool, evidence.get("lineage", True)),
            telemetry_required=cast(bool, evidence.get("telemetry", True)),
            extensions=cast(Mapping[str, JsonValue], extensions),
        )


@dataclass(frozen=True, slots=True)
class Stream:
    ref: ResourceRef
    schema: SchemaRef
    logical_partitions: int = 1
    partition_key_fields: tuple[str, ...] = ()
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    ordering_guarantee: OrderingGuarantee = OrderingGuarantee.PER_LOGICAL_PARTITION
    retention_policy_ref: str = "default"
    policy: ResourcePolicy = field(default_factory=ResourcePolicy)
    format_version: str = RESOURCE_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != RESOURCE_FORMAT_VERSION:
            raise ValueError(f"format_version must be {RESOURCE_FORMAT_VERSION!r}")
        object.__setattr__(self, "ref", _streaming_ref(self.ref, "stream ref"))
        schema = _streaming_schema(self.schema)
        if schema.namespace != self.ref.namespace:
            raise ValueError("Stream and Schema must share a Namespace")
        object.__setattr__(self, "schema", schema)
        if isinstance(self.logical_partitions, bool) or not isinstance(
            self.logical_partitions, int
        ):
            raise TypeError("logical_partitions must be an integer")
        if not 1 <= self.logical_partitions <= MAX_LOGICAL_PARTITIONS:
            raise ValueError("logical_partitions must be between 1 and 1000000")
        keys = tuple(
            bounded_string(item, "partition key field", 256) for item in self.partition_key_fields
        )
        if len(set(keys)) != len(keys):
            raise ValueError("partition_key_fields must be unique")
        object.__setattr__(self, "partition_key_fields", keys)
        if self.delivery_guarantee is not DeliveryGuarantee.AT_LEAST_ONCE:
            raise ValueError("V1 default delivery guarantee is at-least-once")
        if self.ordering_guarantee is not OrderingGuarantee.PER_LOGICAL_PARTITION:
            raise ValueError("V1 ordering is per logical partition")
        object.__setattr__(
            self,
            "retention_policy_ref",
            bounded_string(self.retention_policy_ref, "retention_policy_ref", 512),
        )

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "kind": ResourceKind.STREAM.value,
            "ref": self.ref.to_dict(),
            "schema": self.schema.to_dict(),
            "logicalPartitions": self.logical_partitions,
            "partitionKeyFields": list(self.partition_key_fields),
            "deliveryGuarantee": self.delivery_guarantee.value,
            "orderingGuarantee": self.ordering_guarantee.value,
            "retentionPolicyRef": self.retention_policy_ref,
            "policy": self.policy.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class Subscription:
    ref: ResourceRef
    stream: ResourceRef
    filter: Mapping[str, JsonValue] = field(default_factory=dict)
    acknowledgement_timeout_ms: int = 30_000
    max_delivery_attempts: int = 5
    dead_letter_stream: ResourceRef | None = None
    policy: ResourcePolicy = field(default_factory=ResourcePolicy)
    format_version: str = RESOURCE_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != RESOURCE_FORMAT_VERSION:
            raise ValueError(f"format_version must be {RESOURCE_FORMAT_VERSION!r}")
        object.__setattr__(self, "ref", _streaming_ref(self.ref, "subscription ref"))
        object.__setattr__(self, "stream", _streaming_ref(self.stream, "subscription stream"))
        frozen = freeze_json(self.filter, path="$.filter")
        if not isinstance(frozen, Mapping):
            raise TypeError("filter must be an object")
        object.__setattr__(self, "filter", frozen)
        for value, name, maximum in (
            (self.acknowledgement_timeout_ms, "acknowledgement_timeout_ms", 86_400_000),
            (self.max_delivery_attempts, "max_delivery_attempts", 10_000),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be between 1 and {maximum}")
        if self.dead_letter_stream is not None:
            target = _streaming_ref(self.dead_letter_stream, "dead-letter stream")
            if target == self.stream:
                raise ValueError("dead-letter handling must target another Stream Resource")
            object.__setattr__(self, "dead_letter_stream", target)

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "kind": ResourceKind.SUBSCRIPTION.value,
            "ref": self.ref.to_dict(),
            "stream": self.stream.to_dict(),
            "filter": thaw_json(cast(JsonValue, self.filter)),
            "acknowledgementTimeoutMs": self.acknowledgement_timeout_ms,
            "maxDeliveryAttempts": self.max_delivery_attempts,
            "deadLetterStream": (
                None if self.dead_letter_stream is None else self.dead_letter_stream.to_dict()
            ),
            "policy": self.policy.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ConsumerGroup:
    ref: ResourceRef
    subscription: ResourceRef
    policy: ResourcePolicy = field(default_factory=ResourcePolicy)
    format_version: str = RESOURCE_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != RESOURCE_FORMAT_VERSION:
            raise ValueError(f"format_version must be {RESOURCE_FORMAT_VERSION!r}")
        object.__setattr__(self, "ref", _streaming_ref(self.ref, "consumer-group ref"))
        object.__setattr__(
            self,
            "subscription",
            _streaming_ref(self.subscription, "consumer-group subscription"),
        )

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "kind": ResourceKind.CONSUMER_GROUP.value,
            "ref": self.ref.to_dict(),
            "subscription": self.subscription.to_dict(),
            "policy": self.policy.to_dict(),
        }


StreamingResource = Stream | Subscription | ConsumerGroup


def resource_from_mapping(value: Mapping[str, object]) -> StreamingResource:
    """Parse one exact V1 resource document and reject unknown fields."""

    kind = value.get("kind")
    common = {"formatVersion", "kind", "ref", "policy"}
    if kind == ResourceKind.STREAM.value:
        expected = common | {
            "schema",
            "logicalPartitions",
            "partitionKeyFields",
            "deliveryGuarantee",
            "orderingGuarantee",
            "retentionPolicyRef",
        }
        if set(value) != expected:
            raise ValueError("Stream document contains unknown or missing fields")
        raw_keys = value["partitionKeyFields"]
        if not isinstance(raw_keys, Sequence) or isinstance(raw_keys, str | bytes):
            raise TypeError("partitionKeyFields must be an array")
        return Stream(
            ref=ResourceRef.parse(cast(Mapping[str, object], value["ref"])),
            schema=SchemaRef.parse(cast(Mapping[str, object], value["schema"])),
            logical_partitions=cast(int, value["logicalPartitions"]),
            partition_key_fields=tuple(cast(str, item) for item in raw_keys),
            delivery_guarantee=DeliveryGuarantee(cast(str, value["deliveryGuarantee"])),
            ordering_guarantee=OrderingGuarantee(cast(str, value["orderingGuarantee"])),
            retention_policy_ref=cast(str, value["retentionPolicyRef"]),
            policy=ResourcePolicy.from_mapping(cast(Mapping[str, object], value["policy"])),
            format_version=cast(str, value["formatVersion"]),
        )
    if kind == ResourceKind.SUBSCRIPTION.value:
        expected = common | {
            "stream",
            "filter",
            "acknowledgementTimeoutMs",
            "maxDeliveryAttempts",
            "deadLetterStream",
        }
        if set(value) != expected:
            raise ValueError("Subscription document contains unknown or missing fields")
        raw_dead_letter = value["deadLetterStream"]
        return Subscription(
            ref=ResourceRef.parse(cast(Mapping[str, object], value["ref"])),
            stream=ResourceRef.parse(cast(Mapping[str, object], value["stream"])),
            filter=cast(Mapping[str, JsonValue], value["filter"]),
            acknowledgement_timeout_ms=cast(int, value["acknowledgementTimeoutMs"]),
            max_delivery_attempts=cast(int, value["maxDeliveryAttempts"]),
            dead_letter_stream=(
                None
                if raw_dead_letter is None
                else ResourceRef.parse(cast(Mapping[str, object], raw_dead_letter))
            ),
            policy=ResourcePolicy.from_mapping(cast(Mapping[str, object], value["policy"])),
            format_version=cast(str, value["formatVersion"]),
        )
    if kind == ResourceKind.CONSUMER_GROUP.value:
        expected = common | {"subscription"}
        if set(value) != expected:
            raise ValueError("ConsumerGroup document contains unknown or missing fields")
        return ConsumerGroup(
            ref=ResourceRef.parse(cast(Mapping[str, object], value["ref"])),
            subscription=ResourceRef.parse(cast(Mapping[str, object], value["subscription"])),
            policy=ResourcePolicy.from_mapping(cast(Mapping[str, object], value["policy"])),
            format_version=cast(str, value["formatVersion"]),
        )
    raise ValueError("unknown streaming Resource kind")


__all__ = [
    "CATALOG_NAME",
    "RESOURCE_FORMAT_VERSION",
    "ConsumerGroup",
    "DeliveryGuarantee",
    "OrderingGuarantee",
    "ResourceKind",
    "ResourcePolicy",
    "Stream",
    "StreamingResource",
    "Subscription",
    "resource_from_mapping",
]
