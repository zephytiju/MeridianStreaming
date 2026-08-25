# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from meridian_storage.streaming.canonical import (
    bounded_string,
    canonical_json_bytes,
    contract_token,
    freeze_json,
    sha256_fingerprint,
    string_map,
    thaw_json,
    utc_timestamp,
)


def test_canonical_json_is_order_independent_and_immutable() -> None:
    first = {"z": [1, True, None], "a": {"b": 2.5}}
    second = {"a": {"b": 2.5}, "z": (1, True, None)}
    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    frozen = freeze_json(first)
    assert thaw_json(frozen) == first
    assert sha256_fingerprint(first).startswith("sha256:")


@pytest.mark.parametrize("value", [float("inf"), object(), {1: "bad"}, {"": 1}])
def test_freeze_json_rejects_nonportable_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        freeze_json(value)


def test_bounded_string_and_token_validation() -> None:
    assert bounded_string("e\u0301", "name") == "é"
    assert contract_token("contract.v1", "token") == "contract.v1"
    with pytest.raises(ValueError):
        bounded_string("\x00", "name")
    with pytest.raises(ValueError):
        bounded_string("\x7f", "name")
    with pytest.raises(ValueError):
        contract_token("has space", "token")


def test_timestamp_normalization() -> None:
    assert utc_timestamp("2026-08-25T05:00:00-07:00", "time") == "2026-08-25T12:00:00.000000Z"
    assert utc_timestamp(datetime(2026, 8, 25, 12, tzinfo=UTC), "time").endswith("Z")
    with pytest.raises(ValueError):
        utc_timestamp("2026-08-25T12:00:00", "time")
    with pytest.raises(ValueError, match="RFC 3339"):
        utc_timestamp("not-a-timestamp", "time")
    with pytest.raises(TypeError):
        utc_timestamp(3, "time")  # type: ignore[arg-type]


def test_string_map_is_bounded() -> None:
    assert dict(string_map({"b": "2", "a": "1"}, "metadata")) == {"a": "1", "b": "2"}
    with pytest.raises(ValueError):
        string_map({str(index): "x" for index in range(65)}, "metadata")


def test_json_object_keys_are_normalized_and_collision_safe() -> None:
    with pytest.raises(ValueError, match="collide"):
        freeze_json({"é": 1, "e\u0301": 2})
    with pytest.raises(ValueError, match="invalid object key"):
        freeze_json({"bad\x7f": 1})
