<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture

The package is an independently versioned Catalog library. Consumer mappings
become immutable Core `Expression` values, the installed
`StreamingCatalogProvider` validates and normalizes each Expression to one Core
`Operation`, and Core resolves the logical Resource to one deployment Binding.
Only the selected adapter sees physical placement or connection information.

The package boundary is:

1. `resources.py` defines Stream, Subscription, ConsumerGroup, and cross-cutting
   Resource policy requirements.
2. `data.py` defines Event, Delivery, Position, DeliveryToken, Cursor, finite
   range, and publication Data.
3. `catalog.py` owns the exhaustive mapping-first Expression surface and its
   normalized Operation manifest.
4. `operations.py` owns replay and group-position transitions as explicit
   versioned Operations, outside the Catalog method surface.
5. `capabilities.py` declares guarantees and limits for startup/deployment
   negotiation against an authenticated Core Capability manifest.
6. `hooks.py` defines policy and audit-lineage-telemetry composition hooks.
7. `testing/` ships reusable black-box conformance and a non-production semantic
   oracle.

All values use logical Resource references. Cursor, Position, and DeliveryToken
payloads remain opaque. The visible logical-partition label exists only to state
the ordering scope; it is not a physical coordinate.

## Guarantees

- Default delivery is at-least-once.
- Ordering applies per logical partition, never globally.
- Acknowledgement advances only a validated monotonic safe position.
- Negative acknowledgement makes a Delivery eligible for redelivery.
- Dead-letter policy targets another Stream Resource; failed target publication
  cannot acknowledge the source Delivery.
- Retained-range reads are finite and policy-bounded.
- Replay never silently changes ConsumerGroup position.
- Group-position transitions are explicit, compare-and-set, policy-gated
  Operations.
- OperationContext supplies tenant scope and principal before execution.
- Audit, lineage, and telemetry hooks record logical references and bounded
  fingerprints without payload or deployment secrets.

## Authority boundaries

Meridian owns logical contracts, validation, capability requirements, and
normalized outcomes. Deployment IaC owns Engine selection, identity, network,
retention, replication, recovery, migration, and lifecycle. Provider adapters
own compilation and physical error translation. Consumers own domain payloads,
workflow effects, and idempotent-sink behavior.

Native engine expressions, a stateful processing DSL, a hosted processing
service, scheduling, autoscaling, and cross-Binding transactions are outside V1.
