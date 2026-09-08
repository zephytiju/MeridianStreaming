<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility migration for 1.0.1

Streaming 1.0.0 required Core and Semantics exactly 1.0.0, so a resolver could
not select Core 1.1.0 and the released Semantics 2.0.1. Streaming 1.0.1 repairs
that package composition without changing its public or serialized V1 contracts.

## Dependency boundary

The source inventory finds no direct Semantics imports or private Semantics API
consumption. Streaming consumes the released Core ResourceRef, SchemaRef,
Expression, Operation, OperationContext, CatalogProvider and Capability surfaces.
The Semantics distribution supplies the accompanying logical-schema/Catalog
provider package through its public distribution and entry-point contract.
No Semantics 1 class or field adaptation is required in Streaming code.

Core `>=1.1.0,<2` conservatively starts at the approved release-contract repair.
Semantics `>=2.0.1,<3` starts at the completed public dependency repair and
excludes the previous major series and a future incompatible major. These
bounds express the supported API families; they do not prove arbitrary future
combinations. Untested releases remain unverified. Deployments lock selected
artifacts independently and retain their actual contract and feature checks.

The exact validation recipe is Core 1.1.0 plus Semantics 2.0.1. The packaged
compatibility ledger records their public wheel/sdist SHA-256 hashes and merged
commits; requirements-audit.txt locks the complete runtime dependency closure.
Quality and release CI install that hash-verified recipe and verify installed
versions against it. The Python 3.12, 3.13 and 3.14 matrix separately resolves
package metadata normally and runs the installed package suite plus pip check.
The SPDX SBOM derives dependency provenance from the same validation ledger.
No runtime path reads release versions to decide whether an Operation is valid.

## Contract and conformance scope

The nine mapping-first Expression methods, Operation contract versions, JSON
Schema formats, errors, Stream/Event/Cursor mappings and exact opaque-token
acknowledgement semantics retain their V1 definitions. Existing unit, serialized
contract, reusable provider-conformance and delivery-lifecycle tests remain
required, including partition ordering, replay/group separation, retention,
expiry, acknowledgement recovery and policy denial. Packaging and isolated
installed-wheel checks verify namespace and entry-point ownership.

This provider-neutral library owns the reference target and reusable conformance
suite. Real-engine lifecycle and negotiated feature validation belong to each
Adapter repository and the downstream platform conformance task. This release
makes no new engine or all-family conformance claim.
