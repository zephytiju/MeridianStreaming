<!-- SPDX-License-Identifier: Apache-2.0 -->

# Provider conformance

Provider repositories implement `StreamingConformanceTarget` as a thin bridge
over a configured test Binding and call `run_streaming_conformance`. The bridge
is test-only: it does not define application syntax and cannot replace Core
Operation execution in production.

The mandatory cases are:

- mapping-first method normalization and idempotency classification;
- ordering within each logical partition;
- at-least-once redelivery following negative acknowledgement;
- durable acknowledgement across recovery;
- finite replay that leaves group position unchanged;
- explicit compare-and-set group-position transition;
- opaque Cursor rejection and expiry;
- tenant scope and policy denial;
- audit, lineage, and telemetry evidence hooks; and
- stable normalized failure classes and codes.

A passing report includes target identity, the authenticated Capability
fingerprint, every case outcome, and a canonical report fingerprint. It must not
include credentials, endpoints, physical names, raw provider exceptions, or
domain payload values. Real-engine lifecycle, failover, security, migration,
retention, and version-matrix evidence remains the provider repository's
responsibility and supplements rather than replaces this suite.
