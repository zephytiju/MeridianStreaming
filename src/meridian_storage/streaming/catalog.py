# SPDX-License-Identifier: Apache-2.0
"""Core 1.0.0 provider for the mapping-first Streaming Catalog surface."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast

from meridian_storage import CatalogManifest, Expression, Operation, OperationContract, ResourceRef

from ._version import __version__
from .canonical import JsonValue
from .errors import InvalidStreamingDefinition
from .resources import CATALOG_NAME, ResourceKind
from .validation import parse_resource, validate_expression_arguments

STREAMING_CONTRACT_VERSION = "1.0.0"
STREAMING_REGISTRY_REF = ResourceRef(CATALOG_NAME, "meridian", "registry")

_OPERATION_DEFINITIONS: Mapping[str, tuple[bool, str, tuple[str, ...]]] = MappingProxyType(
    {
        "acknowledge": (
            False,
            "always",
            ("at-least-once", "consumer-groups", "monotonic-safe-position"),
        ),
        "create_resource": (False, "always", ("streaming-resource-lifecycle",)),
        "negative_acknowledge": (
            False,
            "always",
            ("at-least-once", "consumer-groups", "redelivery"),
        ),
        "poll": (
            False,
            "never",
            ("at-least-once", "consumer-groups", "per-logical-partition-ordering"),
        ),
        "publish": (
            False,
            "conditional",
            ("at-least-once", "per-logical-partition-ordering"),
        ),
        "publish_batch": (
            False,
            "conditional",
            ("at-least-once", "per-logical-partition-ordering"),
        ),
        "publish_schema": (False, "always", ("streaming-schema-publication",)),
        "read_range": (True, "always", ("finite-retained-range", "opaque-cursors")),
        "subscribe": (False, "always", ("subscriptions",)),
    }
)


def streaming_manifest() -> CatalogManifest:
    limits: Mapping[str, Mapping[str, int]] = {
        "publish_batch": {"maxBatchSize": 1},
        "poll": {"maxPollSize": 1, "maxWaitTimeoutMs": 0},
        "read_range": {"maxRangeSize": 1},
    }
    return CatalogManifest(
        catalog_name=CATALOG_NAME,
        package_name="meridian-storage-streaming",
        package_version=__version__,
        catalog_contract_version=STREAMING_CONTRACT_VERSION,
        operations=tuple(
            OperationContract(
                method=method,
                operation_contract=f"meridian.streaming.{method.replace('_', '-')}",
                operation_version="1.0.0",
                read_only=read_only,
                idempotency=idempotency,
                guarantees=guarantees,
                minimum_limits=limits.get(method, {}),
            )
            for method, (read_only, idempotency, guarantees) in _OPERATION_DEFINITIONS.items()
        ),
        extensions={
            "design.hldRevision": 56,
            "design.catalogRevision": 70,
            "delivery.default": "at-least-once",
            "ordering.scope": "logical-partition",
            "cursor": "opaque",
            "explicitOperations": [
                "meridian.streaming.replay@1.0.0",
                "meridian.streaming.group-position@1.0.0",
            ],
        },
    )


class StreamingCatalogSurface:
    """The exhaustive V1 mapping-first public Expression surface."""

    catalog_name = CATALOG_NAME

    def publish_schema(
        self,
        *,
        namespace: str,
        name: str,
        version: str,
        definition: Mapping[str, object],
        expected_registry_revision: int | None = None,
        allow_breaking: bool = False,
    ) -> Expression:
        arguments: dict[str, Any] = {
            "namespace": namespace,
            "name": name,
            "version": version,
            "definition": dict(definition),
            "allowBreaking": allow_breaking,
        }
        if expected_registry_revision is not None:
            arguments["expectedRegistryRevision"] = expected_registry_revision
        return self._expression("publish_schema", arguments)

    def create_resource(
        self,
        *,
        namespace: str,
        name: str,
        resource_type: ResourceKind | str,
        schema: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
    ) -> Expression:
        selected = resource_type.value if isinstance(resource_type, ResourceKind) else resource_type
        return self._expression(
            "create_resource",
            {
                "namespace": namespace,
                "name": name,
                "resourceType": selected,
                "schema": None if schema is None else dict(schema),
                "options": dict(options or {}),
            },
        )

    def publish(
        self,
        *,
        resource: str | Mapping[str, object],
        data: Mapping[str, object],
        idempotency_key: str | None = None,
    ) -> Expression:
        arguments: dict[str, Any] = {"resource": resource, "data": dict(data)}
        if idempotency_key is not None:
            arguments["idempotencyKey"] = idempotency_key
        return self._expression("publish", arguments)

    def publish_batch(
        self,
        *,
        resource: str | Mapping[str, object],
        data: Sequence[Mapping[str, object]],
        idempotency_key: str | None = None,
    ) -> Expression:
        arguments: dict[str, Any] = {
            "resource": resource,
            "data": [dict(event) for event in data],
        }
        if idempotency_key is not None:
            arguments["idempotencyKey"] = idempotency_key
        return self._expression("publish_batch", arguments)

    def subscribe(
        self,
        *,
        stream: str | Mapping[str, object],
        subscription: str | Mapping[str, object],
        options: Mapping[str, object] | None = None,
    ) -> Expression:
        return self._expression(
            "subscribe",
            {"stream": stream, "subscription": subscription, "options": dict(options or {})},
        )

    def poll(
        self,
        *,
        subscription: str | Mapping[str, object],
        consumer_group: str | Mapping[str, object],
        limit: int = 100,
        wait_timeout_ms: int = 1_000,
    ) -> Expression:
        return self._expression(
            "poll",
            {
                "subscription": subscription,
                "consumerGroup": consumer_group,
                "limit": limit,
                "waitTimeoutMs": wait_timeout_ms,
            },
        )

    def acknowledge(
        self,
        *,
        subscription: str | Mapping[str, object],
        consumer_group: str | Mapping[str, object],
        delivery: str | Mapping[str, object],
    ) -> Expression:
        return self._expression(
            "acknowledge",
            {
                "subscription": subscription,
                "consumerGroup": consumer_group,
                "delivery": delivery,
            },
        )

    def negative_acknowledge(
        self,
        *,
        subscription: str | Mapping[str, object],
        consumer_group: str | Mapping[str, object],
        delivery: str | Mapping[str, object],
        retry_after_ms: int = 0,
        classification: str | None = None,
    ) -> Expression:
        arguments: dict[str, Any] = {
            "subscription": subscription,
            "consumerGroup": consumer_group,
            "delivery": delivery,
            "retryAfterMs": retry_after_ms,
        }
        if classification is not None:
            arguments["classification"] = classification
        return self._expression("negative_acknowledge", arguments)

    def read_range(
        self,
        *,
        resource: str | Mapping[str, object],
        start: str | Mapping[str, object] | None = None,
        end: str | Mapping[str, object] | None = None,
        cursor: str | Mapping[str, object] | None = None,
        limit: int = 100,
    ) -> Expression:
        return self._expression(
            "read_range",
            {"resource": resource, "start": start, "end": end, "cursor": cursor, "limit": limit},
        )

    def _expression(self, method: str, arguments: Mapping[str, Any]) -> Expression:
        return Expression(CATALOG_NAME, method, cast(Mapping[str, JsonValue], arguments))


class StreamingCatalogProvider:
    """Core ``CatalogProvider`` implementation for the Streaming Catalog."""

    catalog_name = CATALOG_NAME

    def __init__(self) -> None:
        self._manifest = streaming_manifest()

    def manifest(self) -> CatalogManifest:
        return self._manifest

    def create_surface(self) -> StreamingCatalogSurface:
        return StreamingCatalogSurface()

    def normalize(self, expression: Expression) -> Operation:
        """Validate and deterministically normalize one mapping-first Expression."""

        if not isinstance(expression, Expression) or expression.catalog != CATALOG_NAME:
            raise InvalidStreamingDefinition(
                "Expression must belong to the streaming Catalog",
                requirement="expression.catalog",
            )
        try:
            contract = self._manifest.operation_for(expression.method)
        except KeyError as exc:
            raise InvalidStreamingDefinition(
                f"unsupported streaming Expression method {expression.method!r}",
                requirement="expression.method",
            ) from exc

        arguments = cast(Mapping[str, object], expression.arguments)
        validate_expression_arguments(expression.method, arguments)
        resources = _operation_resources(expression.method, arguments)
        if contract.idempotency == "always":
            idempotent = True
        elif contract.idempotency == "never":
            idempotent = False
        else:
            idempotent = "idempotencyKey" in arguments
        return Operation(
            catalog=CATALOG_NAME,
            operation_contract=contract.operation_contract,
            operation_version=contract.operation_version,
            resources=resources,
            input=cast(Mapping[str, JsonValue], arguments),
            requirements=(contract.requirement,),
            read_only=contract.read_only,
            idempotent=idempotent,
        )


def _operation_resources(
    method: str,
    arguments: Mapping[str, object],
) -> tuple[ResourceRef, ...]:
    """Return the complete, canonical Resource set touched by one method."""

    resources: tuple[ResourceRef, ...]
    if method == "publish_schema":
        resources = (STREAMING_REGISTRY_REF,)
    elif method == "create_resource":
        resources = (
            ResourceRef(
                CATALOG_NAME,
                cast(str, arguments["namespace"]),
                cast(str, arguments["name"]),
            ),
        )
    elif method in {"publish", "publish_batch", "read_range"}:
        resources = (parse_resource(arguments["resource"], "resource"),)
    elif method == "subscribe":
        resources = (
            parse_resource(arguments["stream"], "stream"),
            parse_resource(arguments["subscription"], "subscription"),
        )
    elif method in {"poll", "acknowledge", "negative_acknowledge"}:
        resources = (
            parse_resource(arguments["subscription"], "subscription"),
            parse_resource(arguments["consumerGroup"], "consumerGroup"),
        )
    else:  # pragma: no cover - manifest lookup and validation make this unreachable.
        raise InvalidStreamingDefinition(
            f"unsupported streaming Expression method {method!r}",
            requirement="expression.method",
        )
    return tuple(sorted(set(resources)))


__all__ = [
    "STREAMING_CONTRACT_VERSION",
    "STREAMING_REGISTRY_REF",
    "StreamingCatalogProvider",
    "StreamingCatalogSurface",
    "streaming_manifest",
]
