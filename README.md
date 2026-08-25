<!-- SPDX-License-Identifier: Apache-2.0 -->

# Meridian Storage Streaming

[![CI](https://github.com/zephytiju/MeridianStreaming/actions/workflows/ci.yml/badge.svg)](https://github.com/zephytiju/MeridianStreaming/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-%3E%3D3.12-blue.svg)](pyproject.toml)

`meridian-storage-streaming` is the provider-neutral Meridian V1 `streaming`
Catalog package. It owns Stream, Subscription, and ConsumerGroup Resources;
Event, Delivery, Position, and opaque Cursor Data; mapping-first Expressions;
versioned Operations; Capability requirements; normalized failures; policy and
evidence hook contracts; and a reusable black-box conformance suite.

This repository publishes exactly one Python distribution and contributes only
`meridian_storage.streaming` to the shared PEP 420 namespace. It contains no
engine client, engine identifier, deployment configuration, provisioning logic,
or lifecycle authority.

## Install

```console
python -m pip install meridian-storage-streaming==1.0.0
```

Python 3.12 or newer is required. The release pins the published
`meridian-storage-core==1.0.0` and `meridian-storage-semantics==1.0.0`
contracts exactly; their wheel and source-distribution digests are recorded in
[`compatibility.json`](src/meridian_storage/streaming/compatibility.json).

## Mapping-first public interface

```python
streaming = meridian.catalog("streaming")

publish = streaming.publish(
    resource=user_input["stream_resource"],
    data=user_input["event"],
    idempotency_key=user_input.get("idempotency_key"),
)
published = meridian.execute(publish).data

deliveries = meridian.execute(streaming.poll(
    subscription=user_input["subscription_resource"],
    consumer_group=user_input["consumer_group_resource"],
    limit=user_input.get("limit", 100),
    wait_timeout_ms=user_input.get("wait_timeout_ms", 1000),
)).data

for delivery in deliveries:
    handle(delivery["event"])
    meridian.execute(streaming.acknowledge(
        subscription=user_input["subscription_resource"],
        consumer_group=user_input["consumer_group_resource"],
        delivery=delivery["delivery"],
    ))
```

The seven Streaming data-plane methods are exactly `publish`, `publish_batch`,
`subscribe`, `poll`, `acknowledge`, `negative_acknowledge`, and `read_range`.
The released Core registry additionally requires the two common Catalog
lifecycle methods `publish_schema` and `create_resource`; therefore the
installed manifest contains exactly those nine registered methods and no more.

Delivery defaults to at-least-once. Ordering is guaranteed only within one
logical partition. Position, DeliveryToken, and Cursor values are opaque and
must not be decoded by consumers.

## Explicit replay and group position

Replay and ConsumerGroup-position changes are deliberately absent from the
Catalog surface. They are separately authorized, serialized, versioned
Operations:

```python
from meridian_storage.streaming import GroupPositionTransition, ReplayOperation

replay = ReplayOperation(
    stream=user_input["stream_resource"],
    start=user_input["cursor"],
    limit=500,
    authorization_ref="retention-policy/replay",
    reason="incident-recovery",
).to_operation()

transition = GroupPositionTransition(
    subscription=user_input["subscription_resource"],
    consumer_group=user_input["consumer_group_resource"],
    position=user_input["cursor"],
    expected_position_fingerprint=user_input["expected_fingerprint"],
    authorization_ref="administration-policy/group-position",
    reason="approved-recovery",
).to_operation()
```

A replay is finite and never silently changes a ConsumerGroup position. A
position transition is compare-and-set, policy-gated, and evidence-producing.

## Provider conformance

Provider packages supply a small black-box bridge and run the exact same suite:

```python
from meridian_storage.streaming.testing import run_streaming_conformance

report = run_streaming_conformance(target)
assert report.passed
```

The report covers method normalization, per-logical-partition ordering,
at-least-once redelivery, acknowledgement recovery, replay/group separation,
Cursor validation and expiry, tenant and policy enforcement,
audit-lineage-telemetry evidence, and normalized errors. It contains only
logical references and fingerprints.

## Design and security boundary

The implementation is pinned to Meridian HLD revision 56 and Meridian Catalogs
and Public Interfaces revision 70. Deployment IaC selects, provisions, secures,
backs up, migrates, and owns concrete Engines. Core resolves Bindings and
enforces OperationContext scope. This package accepts no endpoint, credential,
physical-resource name, deployment state, or engine-native expression.

The complete boundary is documented in [Architecture](docs/architecture.md),
[Contracts](docs/contracts.md), [Conformance](docs/conformance.md), and
[Security](SECURITY.md).

## Development

```console
python -m pip install -e '.[test]'
ruff format --check src tests scripts
ruff check src tests scripts
mypy src
python scripts/verify_contracts.py
pytest --cov=meridian_storage.streaming --cov-report=term-missing
python -m build
python scripts/verify_artifacts.py dist/*
python scripts/verify_boundaries.py
bandit -c pyproject.toml -r src
pip-audit --require-hashes -r requirements-audit.txt
```

## License

Copyright 2026 Meridian contributors. Licensed under Apache License 2.0; see
[LICENSE](LICENSE) and [NOTICE](NOTICE).
