<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing

1. Verify the version in `pyproject.toml`, `_version.py`, the public API ledger,
   and compatibility document is identical.
2. Run formatting, lint, strict typing, contracts, tests with coverage,
   boundary checks, static security analysis, dependency audit, and builds.
3. Build twice with the same `SOURCE_DATE_EPOCH` and compare artifact hashes.
4. Inspect wheel and source contents, METADATA, Apache-2.0 expression, LICENSE,
   NOTICE, entry point, namespace boundary, and dependencies.
5. Merge a green reviewed pull request without bypassing branch protection.
6. Create the signed `vX.Y.Z` tag. The release workflow rebuilds, attests,
   generates an SPDX SBOM, publishes through the configured trusted publisher,
   and creates a GitHub Release.
7. Verify the public project metadata, wheel import, entry-point discovery,
   provenance, and immutable hashes; then record evidence in the Repository
   Atlas and owning Feishu task.

The first publication requires the repository owner to establish the package
namespace and trusted-publisher relationship. Authentication or MFA must never
be bypassed.
