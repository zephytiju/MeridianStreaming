<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security policy

Report suspected vulnerabilities privately through GitHub Security Advisories
for this repository. Do not include credentials, production endpoints, tenant
payloads, or opaque Cursor and DeliveryToken values in public issues.

Supported releases receive security fixes while they remain in the documented
compatibility matrix. The V1 boundary requires:

- tenant scope and principal supplied through Core `OperationContext`;
- policy authorization before provider compilation;
- deny-by-default deployment authorization;
- opaque deployment secret references;
- bounded mapping inputs and strict unknown-field rejection;
- opaque Cursor, Position, and DeliveryToken Data;
- redacted stable errors;
- audit, lineage, and telemetry evidence without sensitive payload values; and
- no concrete engine dependency or configuration in this distribution.

CI runs static analysis, dependency auditing, contract and boundary checks, and
artifact inspection. Release artifacts carry an SBOM and build provenance.
