# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable

import pytest

from meridian_storage import Expression
from meridian_storage.streaming import (
    InvalidEvent,
    InvalidStreamingDefinition,
    ResourceKind,
    StreamingCatalogProvider,
    StreamingCatalogSurface,
    streaming_manifest,
)


def test_manifest_is_exact_core_surface() -> None:
    manifest = streaming_manifest()
    assert tuple(item.method for item in manifest.operations) == (
        "acknowledge",
        "create_resource",
        "negative_acknowledge",
        "poll",
        "publish",
        "publish_batch",
        "publish_schema",
        "read_range",
        "subscribe",
    )
    assert manifest.extensions["delivery.default"] == "at-least-once"


def test_all_public_methods_normalize() -> None:
    provider = StreamingCatalogProvider()
    surface = provider.create_surface()
    expressions = (
        surface.publish_schema(
            namespace="orders", name="event", version="1.0.0", definition={"fields": []}
        ),
        surface.create_resource(
            namespace="orders",
            name="events",
            resource_type=ResourceKind.STREAM,
            schema={"name": "event", "version": "1.0.0"},
        ),
        surface.publish(resource="orders.events", data={"eventId": "one", "data": {}}),
        surface.publish_batch(
            resource="orders.events",
            data=({"eventId": "one", "data": {}}, {"eventId": "two", "data": {}}),
            idempotency_key="batch-one",
        ),
        surface.subscribe(stream="orders.events", subscription="orders.subscription"),
        surface.poll(subscription="orders.subscription", consumer_group="orders.workers"),
        surface.acknowledge(
            subscription="orders.subscription",
            consumer_group="orders.workers",
            delivery="opaque",
        ),
        surface.negative_acknowledge(
            subscription="orders.subscription",
            consumer_group="orders.workers",
            delivery="opaque",
            classification="retryable",
        ),
        surface.read_range(resource="orders.events", start="opaque-start"),
    )
    operations = tuple(provider.normalize(expression) for expression in expressions)
    assert {operation.operation_contract for operation in operations} == {
        f"meridian.streaming.{method.replace('_', '-')}"
        for method in (
            "publish_schema",
            "create_resource",
            "publish",
            "publish_batch",
            "subscribe",
            "poll",
            "acknowledge",
            "negative_acknowledge",
            "read_range",
        )
    }
    assert operations[3].idempotent
    assert not operations[2].idempotent
    assert operations[-1].read_only


def test_multi_resource_operations_are_deterministic() -> None:
    provider = StreamingCatalogProvider()
    operation = provider.normalize(
        provider.create_surface().poll(
            subscription="orders.subscription", consumer_group="orders.workers"
        )
    )
    assert tuple(str(item) for item in operation.resources) == (
        "streaming:orders.subscription",
        "streaming:orders.workers",
    )


@pytest.mark.parametrize(
    "factory",
    [
        lambda surface: surface.publish_batch(resource="orders.events", data=()),
        lambda surface: surface.poll(
            subscription="orders.subscription", consumer_group="orders.group", limit=0
        ),
        lambda surface: surface.read_range(resource="orders.events", start="one", cursor="other"),
    ],
)
def test_invalid_arguments_fail_during_normalization(
    factory: Callable[[StreamingCatalogSurface], Expression],
) -> None:
    provider = StreamingCatalogProvider()
    with pytest.raises(InvalidStreamingDefinition):
        provider.normalize(factory(provider.create_surface()))


def test_provider_rejects_wrong_catalog_and_unknown_method() -> None:
    provider = StreamingCatalogProvider()
    with pytest.raises(InvalidStreamingDefinition):
        provider.normalize(Expression("structured", "get", {"resource": "orders.events"}))
    with pytest.raises(InvalidStreamingDefinition):
        provider.normalize(Expression("streaming", "replay", {}))


def test_event_provider_fields_are_rejected() -> None:
    provider = StreamingCatalogProvider()
    expression = provider.create_surface().publish(
        resource="orders.events", data={"eventId": "one", "engine": "forbidden"}
    )
    with pytest.raises(InvalidEvent):
        provider.normalize(expression)


def test_publish_idempotency_is_conditional() -> None:
    provider = StreamingCatalogProvider()
    surface = provider.create_surface()
    without_key = provider.normalize(surface.publish(resource="orders.events", data={"id": "1"}))
    with_key = provider.normalize(
        surface.publish(resource="orders.events", data={"id": "1"}, idempotency_key="one")
    )
    assert not without_key.idempotent
    assert with_key.idempotent


def test_normalization_rejects_low_level_type_failures() -> None:
    provider = StreamingCatalogProvider()
    expression = Expression(
        "streaming",
        "poll",
        {
            "subscription": "orders.subscription",
            "consumerGroup": "orders.group",
            "limit": "many",
            "waitTimeoutMs": 0,
        },
    )
    with pytest.raises(InvalidStreamingDefinition) as caught:
        provider.normalize(expression)
    assert caught.value.code == "MERIDIAN_STREAMING_INVALID_DEFINITION"


def test_optional_surface_fields_and_provider_manifest() -> None:
    provider = StreamingCatalogProvider()
    surface = provider.create_surface()
    schema = surface.publish_schema(
        namespace="orders",
        name="event",
        version="1.0.0",
        definition={},
        expected_registry_revision=7,
    )
    assert schema.arguments["expectedRegistryRevision"] == 7
    resource = provider.normalize(
        surface.create_resource(
            namespace="orders",
            name="events",
            resource_type="stream",
        )
    )
    assert resource.resources[0].logical_name == "orders.events"
    assert provider.manifest() is streaming_manifest() or provider.manifest().fingerprint == (
        streaming_manifest().fingerprint
    )
    with pytest.raises(InvalidStreamingDefinition):
        provider.normalize(object())  # type: ignore[arg-type]
