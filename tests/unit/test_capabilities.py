# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage.spi import AdapterDescriptor, CapabilityManifest, OperationCapability
from meridian_storage.streaming import (
    StreamingCapabilityMismatch,
    StreamingDescriptor,
    negotiate_streaming_capabilities,
    require_streaming_capabilities,
    streaming_requirements,
)


def _manifest(*, omit_last: bool = False) -> CapabilityManifest:
    requirements = streaming_requirements()
    capabilities = tuple(
        OperationCapability(
            requirement.operation_contract,
            (requirement.operation_version,),
            requirement.guarantees,
            {name: max(minimum, 10_000) for name, minimum in requirement.minimum_limits.items()},
            cursor_behavior="opaque",
        )
        for requirement in (requirements[:-1] if omit_last else requirements)
    )
    descriptor = AdapterDescriptor(
        "example.streaming",
        "1.0.0",
        "example-driver",
        {"example-engine": ("1.0",)},
        capabilities,
    )
    return CapabilityManifest(descriptor, "example-engine", "1.0")


def test_descriptor_and_successful_negotiation() -> None:
    descriptor = StreamingDescriptor()
    assert descriptor.fingerprint.startswith("sha256:")
    assert descriptor.to_dict()["defaultDelivery"] == "at-least-once"
    manifest = _manifest()
    assert negotiate_streaming_capabilities(manifest) == ()
    require_streaming_capabilities(manifest)


def test_negotiation_reports_all_failures() -> None:
    manifest = _manifest(omit_last=True)
    violations = negotiate_streaming_capabilities(manifest)
    assert len(violations) == 1
    with pytest.raises(StreamingCapabilityMismatch) as caught:
        require_streaming_capabilities(manifest)
    assert caught.value.category.value == "COMPATIBILITY"


def test_descriptor_rejects_duplicate_contracts() -> None:
    requirement = streaming_requirements()[0]
    with pytest.raises(ValueError):
        StreamingDescriptor((requirement, requirement))


def test_descriptor_version_and_requirement_selection() -> None:
    without_explicit = streaming_requirements(include_explicit_transitions=False)
    assert len(without_explicit) == 9
    with pytest.raises(ValueError, match="format_version"):
        StreamingDescriptor(format_version="future")
    assert negotiate_streaming_capabilities(_manifest(), ()) == ()
