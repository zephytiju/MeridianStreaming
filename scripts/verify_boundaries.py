# SPDX-License-Identifier: Apache-2.0
"""Enforce the one-package and provider-neutral consumer boundary."""

from __future__ import annotations

import inspect
import re
import tomllib
from pathlib import Path

from meridian_storage.streaming import StreamingCatalogSurface

ROOT = Path(__file__).resolve().parents[1]


def verify_distribution() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = metadata["project"]
    if project["name"] != "meridian-storage-streaming":
        raise AssertionError("repository must publish only meridian-storage-streaming")
    if project["dependencies"] != [
        "meridian-storage-core==1.0.0",
        "meridian-storage-semantics==1.0.0",
    ]:
        raise AssertionError("runtime dependencies must be the two exact released contracts")
    namespace = ROOT / "src/meridian_storage"
    if (namespace / "__init__.py").exists():
        raise AssertionError("distribution must not compete for the PEP 420 namespace root")
    packages = sorted(path.name for path in namespace.iterdir() if path.is_dir())
    if packages != ["streaming"]:
        raise AssertionError(f"repository contains unexpected Python packages: {packages!r}")


def verify_surface() -> None:
    public = {
        name
        for name, member in inspect.getmembers(StreamingCatalogSurface, inspect.isfunction)
        if not name.startswith("_")
    }
    expected = {
        "acknowledge",
        "create_resource",
        "negative_acknowledge",
        "poll",
        "publish",
        "publish_batch",
        "publish_schema",
        "read_range",
        "subscribe",
    }
    if public != expected:
        raise AssertionError(f"Streaming surface is not exhaustive: {public!r}")


def verify_no_provider_leakage() -> None:
    provider_name = "".join(chr(codepoint) for codepoint in (107, 97, 102, 107, 97))
    vendor_name = "".join(
        chr(codepoint) for codepoint in (99, 111, 110, 102, 108, 117, 101, 110, 116)
    )
    forbidden = (
        provider_name,
        vendor_name + "_" + provider_name,
        "aio" + provider_name,
        "bootstrap" + ".servers",
        "consumer" + ".group.id",
        "topic" + ".name",
        "native" + "query",
    )
    roots = [
        ROOT / "src",
        ROOT / "tests",
        ROOT / "docs",
        ROOT / "README.md",
        ROOT / "pyproject.toml",
    ]
    offenders: list[str] = []
    for root in roots:
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in paths:
            if not path.is_file() or path.suffix in {".pyc", ".whl"}:
                continue
            text = path.read_text(encoding="utf-8").lower()
            hits = [token for token in forbidden if re.search(re.escape(token), text)]
            if hits:
                offenders.append(f"{path.relative_to(ROOT)}: {hits!r}")
    if offenders:
        raise AssertionError("provider leakage detected:\n" + "\n".join(offenders))


def main() -> None:
    verify_distribution()
    verify_surface()
    verify_no_provider_leakage()
    print("repository and provider boundaries verified")


if __name__ == "__main__":
    main()
