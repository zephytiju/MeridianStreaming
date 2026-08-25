# SPDX-License-Identifier: Apache-2.0
"""Deterministic, immutable JSON and timestamp helpers."""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from types import MappingProxyType
from typing import cast

type JsonScalar = bool | int | float | str | None
type JsonValue = JsonScalar | tuple["JsonValue", ...] | Mapping[str, "JsonValue"]

_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/+-]{0,255}$")
_CONTROL_CODEPOINT_LIMIT = 32
_DELETE_CODEPOINT = 127


def _is_control(character: str) -> bool:
    codepoint = ord(character)
    return codepoint < _CONTROL_CODEPOINT_LIMIT or codepoint == _DELETE_CODEPOINT


def bounded_string(value: str, field_name: str, maximum: int = 256) -> str:
    """Validate a bounded, printable, NFC-normalized public string."""

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or len(normalized.encode("utf-8")) > maximum
        or any(_is_control(character) for character in normalized)
    ):
        raise ValueError(f"{field_name} must be a bounded non-empty printable string")
    return normalized


def contract_token(value: str, field_name: str) -> str:
    """Validate a version, guarantee, or other machine-readable token."""

    value = bounded_string(value, field_name)
    if _SAFE_TOKEN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe contract token")
    return value


def freeze_json(value: object, *, path: str = "$") -> JsonValue:
    """Validate JSON compatibility and return a deeply immutable value."""

    if value is None or isinstance(value, bool | str):
        return cast(JsonScalar, value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, JsonValue] = {}
        for raw_key, item in value.items():
            if not isinstance(raw_key, str):
                raise TypeError(f"{path} contains a non-string object key")
            key = unicodedata.normalize("NFC", raw_key)
            if not key or any(_is_control(character) for character in key):
                raise ValueError(f"{path} contains an invalid object key")
            if key in frozen:
                raise ValueError(f"{path} contains keys that collide after NFC normalization")
            frozen[key] = freeze_json(item, path=f"{path}.{key}")
        return MappingProxyType(dict(sorted(frozen.items())))
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return tuple(freeze_json(item, path=f"{path}[{index}]") for index, item in enumerate(value))
    raise TypeError(f"{path} contains a value that is not JSON-compatible")


def thaw_json(value: JsonValue) -> object:
    """Return mutable JSON containers suitable for serialization."""

    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a JSON value using Meridian's deterministic representation."""

    return json.dumps(
        thaw_json(freeze_json(value)),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_fingerprint(value: object) -> str:
    """Return the canonical SHA-256 fingerprint used by public contracts."""

    return f"sha256:{hashlib.sha256(canonical_json_bytes(value)).hexdigest()}"


def utc_timestamp(value: str | datetime, field_name: str) -> str:
    """Normalize an RFC 3339 timestamp to UTC with microsecond precision."""

    if isinstance(value, str):
        text = bounded_string(value, field_name, 64)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an RFC 3339 timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError(f"{field_name} must be a timestamp string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def string_map(
    values: Mapping[str, str], field_name: str, *, maximum: int = 64
) -> Mapping[str, str]:
    """Validate bounded string metadata without exposing mutable input."""

    if not isinstance(values, Mapping) or len(values) > maximum:
        raise ValueError(f"{field_name} must contain at most {maximum} entries")
    normalized = {
        bounded_string(key, f"{field_name} key", 128): bounded_string(
            value, f"{field_name} value", 1024
        )
        for key, value in values.items()
    }
    return MappingProxyType(dict(sorted(normalized.items())))


__all__ = [
    "JsonScalar",
    "JsonValue",
    "bounded_string",
    "canonical_json_bytes",
    "contract_token",
    "freeze_json",
    "sha256_fingerprint",
    "string_map",
    "thaw_json",
    "utc_timestamp",
]
