# SPDX-License-Identifier: Apache-2.0
"""Inspect built wheel/sdist metadata and repository boundaries."""

from __future__ import annotations

import email
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


def _wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = email.message_from_bytes(archive.read(metadata_name))
        if metadata["Name"] != "meridian-storage-streaming" or metadata["Version"] != "1.0.0":
            raise AssertionError("wheel name or version is incorrect")
        if metadata["License-Expression"] != "Apache-2.0":
            raise AssertionError("wheel lacks the Apache-2.0 SPDX license expression")
        if metadata.get_all("Requires-Dist")[:2] != [
            "meridian-storage-core==1.0.0",
            "meridian-storage-semantics==1.0.0",
        ]:
            raise AssertionError("wheel runtime dependencies are not exact compatibility pins")
        modules = {
            PurePosixPath(name).parts[1]
            for name in names
            if name.startswith("meridian_storage/") and len(PurePosixPath(name).parts) > 1
        }
        if modules != {"streaming"}:
            raise AssertionError(f"wheel owns unexpected namespace content: {modules!r}")
        if "meridian_storage/__init__.py" in names:
            raise AssertionError("wheel competes for the shared namespace root")
        required = {
            "meridian_storage_streaming-1.0.0.dist-info/licenses/LICENSE",
            "meridian_storage_streaming-1.0.0.dist-info/licenses/NOTICE",
            "meridian_storage_streaming-1.0.0.dist-info/entry_points.txt",
            "meridian_storage/streaming/py.typed",
            "meridian_storage/streaming/compatibility.json",
        }
        if not required <= set(names):
            raise AssertionError(
                f"wheel is missing required files: {sorted(required - set(names))!r}"
            )
        entry_points = archive.read(
            "meridian_storage_streaming-1.0.0.dist-info/entry_points.txt"
        ).decode("utf-8")
        if (
            "streaming = meridian_storage.streaming.catalog:StreamingCatalogProvider"
            not in entry_points
        ):
            raise AssertionError("wheel does not register the Streaming Catalog provider")


def _sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = archive.getnames()
        if not all(
            name == "meridian_storage_streaming-1.0.0"
            or name.startswith("meridian_storage_streaming-1.0.0/")
            for name in names
        ):
            raise AssertionError("sdist contains a path outside its release root")
        required_suffixes = {"/LICENSE", "/NOTICE", "/pyproject.toml", "/PKG-INFO"}
        for suffix in required_suffixes:
            if not any(name.endswith(suffix) for name in names):
                raise AssertionError(f"sdist is missing {suffix}")


def main(arguments: list[str]) -> None:
    if not arguments:
        raise SystemExit("usage: verify_artifacts.py DIST_FILE [...]")
    kinds: set[str] = set()
    for raw in arguments:
        path = Path(raw)
        if path.suffix == ".whl":
            _wheel(path)
            kinds.add("wheel")
        elif path.name.endswith(".tar.gz"):
            _sdist(path)
            kinds.add("sdist")
        else:
            raise AssertionError(f"unsupported artifact: {path}")
    if kinds != {"wheel", "sdist"}:
        raise AssertionError("verification requires one wheel and one sdist")
    print("release artifacts verified")


if __name__ == "__main__":
    main(sys.argv[1:])
