# SPDX-License-Identifier: Apache-2.0
"""Verify packaged JSON Schemas, fixtures, public surface, and compatibility pins."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from meridian_storage.runtime.operations import REGISTERED_CATALOG_METHODS
from meridian_storage.streaming import (
    StreamingCatalogProvider,
    __version__,
    compatibility_contract,
    public_api_contract,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "src/meridian_storage/streaming"
SCHEMAS = PACKAGE / "contracts/streaming"
FIXTURES = PACKAGE / "contracts/conformance"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_for(path: Path) -> Path:
    if path.name.startswith("stream."):
        return SCHEMAS / "meridian.streaming.resource.v1.schema.json"
    return SCHEMAS / "meridian.streaming.data.v1.schema.json"


def verify_schemas_and_fixtures() -> None:
    for schema_path in sorted(SCHEMAS.glob("*.schema.json")):
        Draft202012Validator.check_schema(_load(schema_path))
    checker = FormatChecker()
    for fixture in sorted((FIXTURES / "valid").glob("*.json")):
        validator = Draft202012Validator(_load(_schema_for(fixture)), format_checker=checker)
        validator.validate(_load(fixture))
    for fixture in sorted((FIXTURES / "invalid").glob("*.json")):
        validator = Draft202012Validator(_load(_schema_for(fixture)), format_checker=checker)
        try:
            validator.validate(_load(fixture))
        except ValidationError:
            continue
        raise AssertionError(f"invalid fixture unexpectedly passed: {fixture.name}")


def verify_public_surface() -> None:
    contract = public_api_contract()
    expected = tuple(REGISTERED_CATALOG_METHODS["streaming"])
    actual = tuple(contract["expressionMethods"])
    if actual != expected:
        raise AssertionError(f"public API ledger differs from Core: {actual!r} != {expected!r}")
    manifest = StreamingCatalogProvider().manifest()
    if tuple(item.method for item in manifest.operations) != expected:
        raise AssertionError("provider manifest differs from the Core registry")
    if manifest.catalog_name != "streaming" or manifest.catalog_contract_version != "1.0.0":
        raise AssertionError("provider manifest identity is invalid")


def verify_compatibility() -> None:
    contract = compatibility_contract()
    if contract["design"] != {"catalogsRevision": 70, "hldRevision": 56}:
        raise AssertionError("design pins do not match the locked baseline")
    for dependency in ("core", "semantics"):
        entry = contract[dependency]
        if entry["version"] != "1.0.0":
            raise AssertionError(f"{dependency} is not pinned to 1.0.0")
        for field in ("sdistSha256", "wheelSha256"):
            if SHA256.fullmatch(entry[field]) is None:
                raise AssertionError(f"{dependency}.{field} is not a SHA-256 digest")
        if COMMIT.fullmatch(entry["publicContractCommit"]) is None:
            raise AssertionError(f"{dependency} public contract commit is invalid")


def verify_distribution_identity() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    public = public_api_contract()
    compatibility = compatibility_contract()
    versions = {project["version"], public["version"], compatibility["version"], __version__}
    if versions != {"1.0.0"}:
        raise AssertionError(f"distribution versions are inconsistent: {versions!r}")
    if {
        project["name"],
        public["distribution"],
        compatibility["distribution"],
    } != {"meridian-storage-streaming"}:
        raise AssertionError("distribution identities are inconsistent")


def main() -> None:
    verify_schemas_and_fixtures()
    verify_public_surface()
    verify_compatibility()
    verify_distribution_identity()
    print("streaming contracts verified")


if __name__ == "__main__":
    main()
