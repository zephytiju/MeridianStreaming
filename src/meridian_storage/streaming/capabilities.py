# SPDX-License-Identifier: Apache-2.0
"""Streaming descriptor and Adapter Capability negotiation helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from types import MappingProxyType

from meridian_storage.registry import CapabilityRequirement
from meridian_storage.spi.capabilities import (
    CapabilityManifest,
    CapabilityViolation,
    capability_violations,
)

from .canonical import sha256_fingerprint
from .catalog import streaming_manifest
from .errors import StreamingCapabilityMismatch
from .operations import (
    EXPLICIT_OPERATION_VERSION,
    GROUP_POSITION_OPERATION_CONTRACT,
    REPLAY_OPERATION_CONTRACT,
)

CAPABILITY_CONTRACT_VERSION = "meridian.streaming.capabilities.v1"


def streaming_requirements(
    *, include_explicit_transitions: bool = True
) -> tuple[CapabilityRequirement, ...]:
    requirements = [contract.requirement for contract in streaming_manifest().operations]
    if include_explicit_transitions:
        requirements.extend(
            (
                CapabilityRequirement(
                    REPLAY_OPERATION_CONTRACT,
                    EXPLICIT_OPERATION_VERSION,
                    ("explicit-replay", "finite-retained-range", "opaque-cursors"),
                    {"maxRangeSize": 1},
                ),
                CapabilityRequirement(
                    GROUP_POSITION_OPERATION_CONTRACT,
                    EXPLICIT_OPERATION_VERSION,
                    ("consumer-groups", "explicit-group-position", "opaque-cursors"),
                ),
            )
        )
    return tuple(sorted(requirements, key=lambda item: item.operation_contract))


@dataclass(frozen=True, slots=True)
class StreamingDescriptor:
    """Provider-neutral descriptor consumed by adapters and deployment validation."""

    requirements: tuple[CapabilityRequirement, ...] = streaming_requirements()
    format_version: str = CAPABILITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != CAPABILITY_CONTRACT_VERSION:
            raise ValueError(f"format_version must be {CAPABILITY_CONTRACT_VERSION!r}")
        ordered = tuple(sorted(self.requirements, key=lambda item: item.operation_contract))
        if len({item.operation_contract for item in ordered}) != len(ordered):
            raise ValueError("Streaming descriptor requirements must be unique")
        object.__setattr__(self, "requirements", ordered)

    @property
    def fingerprint(self) -> str:
        return sha256_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "catalog": "streaming",
            "defaultDelivery": "at-least-once",
            "ordering": "per-logical-partition",
            "cursor": "opaque",
            "requirements": [item.to_dict() for item in self.requirements],
        }


def negotiate_streaming_capabilities(
    manifest: CapabilityManifest,
    requirements: Iterable[CapabilityRequirement] | None = None,
) -> tuple[CapabilityViolation, ...]:
    """Return complete, deterministic mismatch evidence for one authenticated manifest."""

    selected = streaming_requirements() if requirements is None else requirements
    return capability_violations(manifest, selected)


def require_streaming_capabilities(
    manifest: CapabilityManifest,
    requirements: Iterable[CapabilityRequirement] | None = None,
) -> None:
    violations = negotiate_streaming_capabilities(manifest, requirements)
    if not violations:
        return
    summary = MappingProxyType(
        {violation.requirement.operation_contract: violation.reason for violation in violations}
    )
    raise StreamingCapabilityMismatch(
        f"Adapter Capability manifest does not satisfy Streaming requirements: {dict(summary)!r}",
        requirement="streaming.capabilities",
        adapter_provenance={
            "adapterId": manifest.adapter_id,
            "capabilityFingerprint": manifest.fingerprint,
        },
    )


__all__ = [
    "CAPABILITY_CONTRACT_VERSION",
    "StreamingDescriptor",
    "negotiate_streaming_capabilities",
    "require_streaming_capabilities",
    "streaming_requirements",
]
