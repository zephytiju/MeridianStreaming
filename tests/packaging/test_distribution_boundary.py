# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib.metadata
import inspect
from pathlib import Path

import pytest
from packaging.requirements import Requirement

from meridian_storage import streaming
from meridian_storage.spi import CatalogProvider

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.packaging
def test_distribution_identity_dependencies_and_entry_point() -> None:
    distribution = importlib.metadata.distribution("meridian-storage-streaming")
    assert distribution.version == "1.0.1"
    assert distribution.metadata["License-Expression"] == "Apache-2.0"
    runtime_requirements = {
        requirement
        for requirement in distribution.requires or ()
        if "; extra ==" not in requirement
    }
    assert runtime_requirements == {
        "meridian-storage-core<2,>=1.1.0",
        "meridian-storage-semantics<3,>=2.0.1",
    }
    points = [
        point
        for point in importlib.metadata.entry_points(group="meridian_storage.catalogs")
        if point.name == "streaming"
    ]
    assert len(points) == 1
    provider = points[0].load()()
    assert isinstance(provider, CatalogProvider)


@pytest.mark.packaging
def test_repository_and_namespace_own_exactly_one_package() -> None:
    namespace = ROOT / "src" / "meridian_storage"
    assert not (namespace / "__init__.py").exists()
    assert sorted(path.name for path in namespace.iterdir() if path.is_dir()) == ["streaming"]
    assert streaming.__version__ == "1.0.1"
    assert set(streaming.__all__) == {
        name for name in streaming.__all__ if hasattr(streaming, name)
    }


@pytest.mark.packaging
def test_catalog_surface_has_no_extra_public_methods() -> None:
    methods = {
        name
        for name, member in inspect.getmembers(
            streaming.StreamingCatalogSurface,
            inspect.isfunction,
        )
        if not name.startswith("_")
    }
    assert methods == {
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


@pytest.mark.packaging
@pytest.mark.parametrize(
    ("name", "accepted", "rejected"),
    [
        ("meridian-storage-core", ("1.1.0", "1.1.1", "1.2.0"), ("1.0.0", "2.0.0")),
        ("meridian-storage-semantics", ("2.0.1", "2.0.2", "2.1.0"), ("1.0.0", "3.0.0")),
    ],
)
def test_dependency_ranges_keep_major_contract_boundaries(
    name: str, accepted: tuple[str, ...], rejected: tuple[str, ...]
) -> None:
    """Synthetic coordinates verify metadata only, not untested release behavior."""
    requirements = {
        req.name: req
        for req in map(Requirement, importlib.metadata.requires("meridian-storage-streaming") or ())
        if req.marker is None
    }
    specifier = requirements[name].specifier
    assert all(version in specifier for version in accepted)
    assert all(version not in specifier for version in rejected)
