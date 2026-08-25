# SPDX-License-Identifier: Apache-2.0
"""Policy and audit-lineage-telemetry hook contracts for Streaming execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, cast, runtime_checkable

from meridian_storage import Operation, OperationContext

from .canonical import JsonValue, bounded_string, freeze_json, thaw_json, utc_timestamp


class EvidenceKind(StrEnum):
    AUDIT = "audit"
    LINEAGE = "lineage"
    TELEMETRY = "telemetry"


@dataclass(frozen=True, slots=True)
class StreamingEvidence:
    kind: EvidenceKind
    operation_contract: str
    occurred_at: str
    outcome: str
    attributes: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_contract",
            bounded_string(self.operation_contract, "operation_contract", 256),
        )
        object.__setattr__(self, "occurred_at", utc_timestamp(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "outcome", bounded_string(self.outcome, "outcome", 128))
        frozen = freeze_json(self.attributes, path="$.attributes")
        if not isinstance(frozen, Mapping):
            raise TypeError("evidence attributes must be an object")
        object.__setattr__(self, "attributes", frozen)

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "operationContract": self.operation_contract,
            "occurredAt": self.occurred_at,
            "outcome": self.outcome,
            "attributes": thaw_json(cast(JsonValue, self.attributes)),
        }


@runtime_checkable
class StreamingPolicyHook(Protocol):
    def authorize(self, context: OperationContext, operation: Operation) -> None: ...


@runtime_checkable
class StreamingEvidenceHook(Protocol):
    def emit(self, context: OperationContext, evidence: StreamingEvidence) -> None: ...


@dataclass(frozen=True, slots=True)
class StreamingHooks:
    """Composition-root hooks; adapters cannot bypass policy or required evidence."""

    policy: StreamingPolicyHook
    audit: StreamingEvidenceHook
    lineage: StreamingEvidenceHook
    telemetry: StreamingEvidenceHook

    def authorize(self, context: OperationContext, operation: Operation) -> None:
        self.policy.authorize(context, operation)

    def emit(self, context: OperationContext, evidence: StreamingEvidence) -> None:
        sinks = {
            EvidenceKind.AUDIT: self.audit,
            EvidenceKind.LINEAGE: self.lineage,
            EvidenceKind.TELEMETRY: self.telemetry,
        }
        sinks[evidence.kind].emit(context, evidence)


__all__ = [
    "EvidenceKind",
    "StreamingEvidence",
    "StreamingEvidenceHook",
    "StreamingHooks",
    "StreamingPolicyHook",
]
