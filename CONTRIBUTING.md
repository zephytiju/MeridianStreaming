<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing

Changes must preserve the one-repository/one-distribution boundary, the closed
five-Catalog registry, mapping-first syntax, opaque Cursor semantics,
at-least-once default delivery, and per-logical-partition ordering. Do not add a
concrete engine import, identifier, configuration field, lifecycle action, or
native-expression surface.

Install `.[test]`, run every command in the README development section, and add
unit, contract, conformance, packaging, and lifecycle coverage proportional to
the change. Serialized or public-surface changes require compatibility fixtures
and a semantic-version decision.
