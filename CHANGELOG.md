<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

## 1.0.1 — 2026-09-08

- Admit public Core `>=1.1.0,<2` and Semantics `>=2.0.1,<3` dependencies.
- Separate compatible package metadata from the exact hashed release-validation recipe.
- Preserve V1 Event, Cursor, ordering, acknowledgement, replay and expiry contracts.
- Validate normal public installs on Python 3.12–3.14 and retain all release gates.

## 1.0.0 — 2026-08-25

- Initial provider-neutral Meridian V1 Streaming Catalog.
- Added Stream, Subscription, ConsumerGroup, Event, Delivery, Position, and
  opaque Cursor contracts.
- Added the exhaustive mapping-first public Expression surface and serialized
  Operation contracts.
- Added explicit replay and group-position Operations, Capability negotiation,
  normalized errors, policy/evidence hooks, and reusable conformance evidence.
