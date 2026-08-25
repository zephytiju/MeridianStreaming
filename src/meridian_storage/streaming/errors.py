# SPDX-License-Identifier: Apache-2.0
"""Stable, redacted failures for the provider-neutral Streaming Catalog."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from meridian_storage import (
    AuthorizationError,
    CompatibilityError,
    ConflictError,
    ConstraintError,
    MeridianTimeoutError,
    NotFoundError,
    UnavailableError,
    ValidationError,
)


class _StreamingDetails:
    requirement: str | None
    logical_references: tuple[str, ...]

    def _set_streaming_details(
        self,
        *,
        requirement: str | None,
        logical_references: tuple[str, ...],
    ) -> None:
        self.requirement = requirement
        self.logical_references = tuple(sorted(set(logical_references)))

    def to_dict(self) -> dict[str, Any]:
        payload = cast(dict[str, Any], super().to_dict())  # type: ignore[misc]
        if self.requirement is not None:
            payload["requirement"] = self.requirement
        if self.logical_references:
            payload["logicalReferences"] = list(self.logical_references)
        return payload


def _details(
    details: Mapping[str, Any],
) -> tuple[dict[str, Any], str | None, tuple[str, ...]]:
    copied = dict(details)
    requirement = copied.pop("requirement", None)
    references = copied.pop("logical_references", ())
    if requirement is not None and not isinstance(requirement, str):
        raise TypeError("requirement must be a string")
    if not isinstance(references, tuple) or not all(isinstance(item, str) for item in references):
        raise TypeError("logical_references must be a tuple of strings")
    return copied, requirement, references


class InvalidStreamingDefinition(_StreamingDetails, ValidationError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_INVALID_DEFINITION", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class InvalidEvent(_StreamingDetails, ValidationError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_EVENT_INVALID", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class InvalidCursor(_StreamingDetails, ValidationError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_CURSOR_INVALID", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class CursorExpired(_StreamingDetails, CompatibilityError):
    def __init__(
        self, message: str = "Cursor is outside its valid retained range", **details: Any
    ) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_CURSOR_EXPIRED", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class DeliveryRejected(_StreamingDetails, ValidationError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_DELIVERY_INVALID", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class DeliveryTimeout(_StreamingDetails, MeridianTimeoutError):
    def __init__(
        self, message: str = "Delivery acknowledgement deadline elapsed", **details: Any
    ) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_DELIVERY_TIMEOUT", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class AcknowledgementConflict(_StreamingDetails, ConflictError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_ACKNOWLEDGEMENT_CONFLICT", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class GroupPositionConflict(_StreamingDetails, ConflictError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_GROUP_POSITION_CONFLICT", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class RebalanceConflict(_StreamingDetails, ConflictError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        core.setdefault("retryable", True)
        super().__init__("MERIDIAN_STREAMING_REBALANCE_CONFLICT", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class RetentionBoundaryExceeded(_StreamingDetails, ConstraintError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_RETENTION_BOUNDARY", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class StreamingCapabilityMismatch(_StreamingDetails, CompatibilityError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_CAPABILITY_MISMATCH", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class StreamingResourceNotFound(_StreamingDetails, NotFoundError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_RESOURCE_NOT_FOUND", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class StreamingPolicyDenied(_StreamingDetails, AuthorizationError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_POLICY_DENIED", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class StreamingUnavailable(_StreamingDetails, UnavailableError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        core.setdefault("retryable", True)
        super().__init__("MERIDIAN_STREAMING_UNAVAILABLE", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


class MigrationRequired(_StreamingDetails, CompatibilityError):
    def __init__(self, message: str, **details: Any) -> None:
        core, requirement, references = _details(details)
        super().__init__("MERIDIAN_STREAMING_MIGRATION_REQUIRED", message, **core)
        self._set_streaming_details(requirement=requirement, logical_references=references)


__all__ = [
    "AcknowledgementConflict",
    "CursorExpired",
    "DeliveryRejected",
    "DeliveryTimeout",
    "GroupPositionConflict",
    "InvalidCursor",
    "InvalidEvent",
    "InvalidStreamingDefinition",
    "MigrationRequired",
    "RebalanceConflict",
    "RetentionBoundaryExceeded",
    "StreamingCapabilityMismatch",
    "StreamingPolicyDenied",
    "StreamingResourceNotFound",
    "StreamingUnavailable",
]
