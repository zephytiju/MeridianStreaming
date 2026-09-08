<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contracts

The language-neutral JSON documents are packaged under
`meridian_storage.streaming.contracts` and available through
`contract_document()`, `public_api_contract()`, and
`compatibility_contract()`.

## Registered Expressions

The Core-mandated Catalog lifecycle methods are `publish_schema` and
`create_resource`. The Streaming data-plane methods are `publish`,
`publish_batch`, `subscribe`, `poll`, `acknowledge`,
`negative_acknowledge`, and `read_range`. Every method returns a Core
`Expression`; no method executes a provider directly.

Each normalized public Operation uses version `1.0.0` and a stable contract
name under `meridian.streaming.*`. Capability requirements state the full
guarantee, not an implementation preference. Publication becomes idempotent
only when an idempotency key is present. Poll may create delivery state and is
therefore not classified as read-only. Acknowledgement and negative
acknowledgement are idempotent by opaque delivery token.

## Explicit Operations

`meridian.streaming.replay@1.0.0` performs a finite retained-range replay and
sets `mutatesConsumerGroup` to false. `meridian.streaming.group-position@1.0.0`
performs a compare-and-set position transition using an expected fingerprint,
an authorization reference, and a reason. Neither is registered as a Catalog
method.

## Resources and Data

Stream fixes V1 delivery and ordering guarantees and binds to an exact
streaming Schema. Subscription selects one Stream and owns filtering,
acknowledgement timeout, bounded delivery attempts, and optional dead-letter
target. ConsumerGroup binds to a Subscription without exposing an engine group
identifier.

Event carries exact logical Stream and Schema references, mapping Data,
timestamps, a logical partition key, bounded headers, trace correlation, and
extensions. Cursor, Position, and DeliveryToken wrap opaque values without any
decoder. All timestamp serialization is UTC RFC 3339 with microseconds.

## Compatibility

`compatibility.json` records the exact public Core 1.1.0 / Semantics 2.0.1
validation recipe, artifact hashes and commits. Its `requires` fields describe
the package metadata bounds; the exact versions are conformance evidence, not
runtime acceptance gates. Historical design revisions record the V1 origin.
A release that changes a serialized field, registered method, stable error code,
guarantee, or explicit Operation contract requires semantic-version review.
See [Compatibility migration](compatibility.md) for dependency and test scope.
