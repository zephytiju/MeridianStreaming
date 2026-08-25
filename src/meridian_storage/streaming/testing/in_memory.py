# SPDX-License-Identifier: Apache-2.0
"""Deterministic reference target for the reusable conformance runner."""

from __future__ import annotations

from dataclasses import dataclass

from ..canonical import sha256_fingerprint
from ..errors import (
    CursorExpired,
    DeliveryRejected,
    GroupPositionConflict,
    InvalidCursor,
    InvalidStreamingDefinition,
    StreamingPolicyDenied,
)
from .conformance import ConformanceDelivery, ConformanceRange


@dataclass(slots=True)
class _Stored:
    event_id: str
    partition: str
    sequence: int
    attempt: int = 0
    inflight: bool = False
    acknowledged: bool = False


class InMemoryStreamingTarget:
    """Non-production semantic oracle; it owns no provider integration."""

    target_id = "meridian.reference.in-memory"

    def __init__(self) -> None:
        self.reset()

    @property
    def capability_fingerprint(self) -> str:
        return sha256_fingerprint(
            {
                "target": self.target_id,
                "delivery": "at-least-once",
                "ordering": "per-logical-partition",
            }
        )

    def reset(self) -> None:
        self._events: list[_Stored] = []
        self._tokens: dict[str, _Stored] = {}
        self._cursors: dict[str, int] = {}
        self._expired: set[str] = set()
        self._evidence: list[str] = []
        self._position = 0

    def publish(
        self,
        *,
        event_id: str,
        logical_partition: str,
        sequence: int,
        tenant: str,
        authorized: bool = True,
    ) -> None:
        if not authorized:
            raise StreamingPolicyDenied(
                "OperationContext principal is not authorized for the tenant scope",
                requirement="operation.policy",
            )
        if not tenant:
            raise StreamingPolicyDenied("tenant scope is required", requirement="operation.tenant")
        self._events.append(_Stored(event_id, logical_partition, sequence))
        self._evidence.extend(("audit", "lineage", "telemetry"))

    def poll(self, *, limit: int = 100) -> tuple[ConformanceDelivery, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise InvalidStreamingDefinition("poll limit must be positive")
        deliveries: list[ConformanceDelivery] = []
        for index, event in enumerate(self._events):
            if event.acknowledged or event.inflight:
                continue
            event.attempt += 1
            event.inflight = True
            token = f"delivery-{index}-{event.attempt}"
            self._tokens[token] = event
            deliveries.append(
                ConformanceDelivery(
                    event.event_id,
                    event.partition,
                    event.sequence,
                    token,
                    event.attempt,
                )
            )
            if len(deliveries) >= limit:
                break
        return tuple(deliveries)

    def acknowledge(self, token: str) -> None:
        event = self._tokens.get(token)
        if event is None or not event.inflight:
            raise DeliveryRejected("delivery token is unknown or no longer active")
        event.acknowledged = True
        event.inflight = False
        self._position = max(self._position, self._events.index(event) + 1)
        self._evidence.extend(("audit", "telemetry"))

    def negative_acknowledge(self, token: str) -> None:
        event = self._tokens.get(token)
        if event is None or not event.inflight:
            raise DeliveryRejected("delivery token is unknown or no longer active")
        event.inflight = False
        self._evidence.extend(("audit", "telemetry"))

    def recover(self) -> None:
        for event in self._events:
            if event.inflight and not event.acknowledged:
                event.inflight = False

    def read_range(self, cursor: str | None = None, *, limit: int = 100) -> ConformanceRange:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise InvalidStreamingDefinition("range limit must be positive")
        start = 0
        if cursor is not None:
            if cursor in self._expired:
                raise CursorExpired(requirement="cursor.retained-range")
            try:
                start = self._cursors[cursor]
            except KeyError as exc:
                raise InvalidCursor("Cursor is not recognized by this target") from exc
        selected = self._events[start : start + limit]
        next_index = start + len(selected)
        next_cursor = f"cursor-{next_index}"
        self._cursors[next_cursor] = next_index
        return ConformanceRange(
            tuple(event.event_id for event in selected),
            next_cursor,
            next_index < len(self._events),
        )

    def replay(self, cursor: str) -> ConformanceRange:
        before = self._position
        result = self.read_range(cursor)
        if self._position != before:  # pragma: no cover - reference-oracle invariant.
            raise AssertionError("replay mutated ConsumerGroup position")
        return result

    def group_position_fingerprint(self) -> str:
        return sha256_fingerprint({"position": self._position})

    def transition_group_position(self, cursor: str, expected_fingerprint: str) -> None:
        if expected_fingerprint != self.group_position_fingerprint():
            raise GroupPositionConflict("group position compare-and-set failed")
        if cursor in self._expired:
            raise CursorExpired(requirement="cursor.retained-range")
        try:
            self._position = self._cursors[cursor]
        except KeyError as exc:
            raise InvalidCursor("Cursor is not recognized by this target") from exc
        self._evidence.extend(("audit", "lineage", "telemetry"))

    def expire_cursor(self, cursor: str) -> None:
        if cursor not in self._cursors:
            raise InvalidCursor("Cursor is not recognized by this target")
        self._expired.add(cursor)

    def evidence_kinds(self) -> tuple[str, ...]:
        return tuple(self._evidence)


__all__ = ["InMemoryStreamingTarget"]
