# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage import ResourceRef, SchemaRef
from meridian_storage.streaming import (
    ConsumerGroup,
    DeliveryGuarantee,
    OrderingGuarantee,
    ResourcePolicy,
    Stream,
    Subscription,
    resource_from_mapping,
)


def test_stream_round_trip(stream_ref: ResourceRef, schema_ref: SchemaRef) -> None:
    resource = Stream(
        stream_ref,
        schema_ref,
        logical_partitions=8,
        partition_key_fields=("order_id",),
        retention_policy_ref="standard",
    )
    assert resource.delivery_guarantee is DeliveryGuarantee.AT_LEAST_ONCE
    assert resource.ordering_guarantee is OrderingGuarantee.PER_LOGICAL_PARTITION
    assert resource_from_mapping(resource.to_dict()) == resource
    assert resource.fingerprint.startswith("sha256:")


def test_subscription_and_group_round_trip(
    stream_ref: ResourceRef,
    subscription_ref: ResourceRef,
    group_ref: ResourceRef,
) -> None:
    dead_letter = ResourceRef("streaming", "orders", "failures")
    subscription = Subscription(
        subscription_ref,
        stream_ref,
        {"eventType": {"eq": "created"}},
        dead_letter_stream=dead_letter,
        policy=ResourcePolicy(policy_ref="orders-policy"),
    )
    group = ConsumerGroup(group_ref, subscription_ref)
    assert resource_from_mapping(subscription.to_dict()) == subscription
    assert resource_from_mapping(group.to_dict()) == group
    assert subscription.fingerprint != group.fingerprint


def test_resource_policy_defaults_and_mapping() -> None:
    policy = ResourcePolicy.from_mapping(None)
    assert policy.required_scope == ("tenant",)
    assert ResourcePolicy.from_mapping(policy.to_dict()) == policy
    with pytest.raises(ValueError):
        ResourcePolicy.from_mapping({"unknown": True})


@pytest.mark.parametrize("logical_partitions", [0, 1_000_001, True])
def test_stream_rejects_invalid_partition_count(
    stream_ref: ResourceRef,
    schema_ref: SchemaRef,
    logical_partitions: int,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        Stream(stream_ref, schema_ref, logical_partitions=logical_partitions)


def test_subscription_dead_letter_must_be_another_stream(
    stream_ref: ResourceRef,
    subscription_ref: ResourceRef,
) -> None:
    with pytest.raises(ValueError):
        Subscription(subscription_ref, stream_ref, dead_letter_stream=stream_ref)


def test_resource_parser_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError):
        resource_from_mapping({"kind": "unknown"})


def test_resource_policy_rejects_invalid_scope_evidence_and_extensions() -> None:
    with pytest.raises(ValueError, match="unique"):
        ResourcePolicy(("tenant", "tenant"))
    with pytest.raises(TypeError, match="boolean"):
        ResourcePolicy(audit_required=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="object"):
        ResourcePolicy(extensions=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="array"):
        ResourcePolicy.from_mapping({"requiredScope": "tenant"})
    with pytest.raises(TypeError, match="objects"):
        ResourcePolicy.from_mapping({"evidence": []})
    with pytest.raises(ValueError, match="unknown"):
        ResourcePolicy.from_mapping({"evidence": {"unknown": True}})


def test_stream_rejects_invalid_contract_combinations(stream_ref: ResourceRef) -> None:
    schema = SchemaRef("streaming", "other", "event", "1.0.0")
    with pytest.raises(ValueError, match="Namespace"):
        Stream(stream_ref, schema)
    with pytest.raises(ValueError, match="format_version"):
        Stream(
            stream_ref,
            SchemaRef("streaming", "orders", "event", "1.0.0"),
            format_version="future",
        )
    with pytest.raises(ValueError, match="unique"):
        Stream(
            stream_ref,
            SchemaRef("streaming", "orders", "event", "1.0.0"),
            partition_key_fields=("id", "id"),
        )
    with pytest.raises(ValueError, match="at-least-once"):
        Stream(
            stream_ref,
            SchemaRef("streaming", "orders", "event", "1.0.0"),
            delivery_guarantee="other",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="logical partition"):
        Stream(
            stream_ref,
            SchemaRef("streaming", "orders", "event", "1.0.0"),
            ordering_guarantee="global",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("acknowledgement_timeout_ms", 0), ("max_delivery_attempts", 0)],
)
def test_subscription_rejects_invalid_limits(
    stream_ref: ResourceRef,
    subscription_ref: ResourceRef,
    field: str,
    value: int,
) -> None:
    arguments = {field: value}
    with pytest.raises(ValueError):
        Subscription(subscription_ref, stream_ref, **arguments)  # type: ignore[arg-type]


def test_subscription_and_group_reject_invalid_envelopes(
    stream_ref: ResourceRef,
    subscription_ref: ResourceRef,
    group_ref: ResourceRef,
) -> None:
    with pytest.raises(TypeError, match="object"):
        Subscription(subscription_ref, stream_ref, filter=[])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="format_version"):
        Subscription(subscription_ref, stream_ref, format_version="future")
    with pytest.raises(ValueError, match="format_version"):
        ConsumerGroup(group_ref, subscription_ref, format_version="future")


def test_resource_parser_rejects_shape_errors(
    stream_ref: ResourceRef,
    schema_ref: SchemaRef,
) -> None:
    stream = Stream(stream_ref, schema_ref).to_dict()
    stream["unknown"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        resource_from_mapping(stream)
    stream = Stream(stream_ref, schema_ref).to_dict()
    stream["partitionKeyFields"] = "id"
    with pytest.raises(TypeError, match="array"):
        resource_from_mapping(stream)
    with pytest.raises(ValueError, match="unknown or missing"):
        resource_from_mapping(
            {
                "kind": "subscription",
                "formatVersion": "meridian.streaming.resource.v1",
                "ref": stream_ref.to_dict(),
                "policy": ResourcePolicy().to_dict(),
            }
        )
    with pytest.raises(ValueError, match="unknown or missing"):
        resource_from_mapping(
            {
                "kind": "consumer-group",
                "formatVersion": "meridian.streaming.resource.v1",
                "ref": stream_ref.to_dict(),
                "policy": ResourcePolicy().to_dict(),
            }
        )
