# SPDX-License-Identifier: Apache-2.0
"""Access to the released language-neutral Streaming contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import resources
from typing import cast


def contract_document(name: str) -> Mapping[str, object]:
    if "/" in name or "\\" in name or not name.endswith(".json"):
        raise ValueError("contract name must be a JSON filename")
    target = resources.files("meridian_storage.streaming").joinpath("contracts", "streaming", name)
    if not target.is_file():
        raise FileNotFoundError(name)
    return cast(Mapping[str, object], json.loads(target.read_text(encoding="utf-8")))


def public_api_contract() -> Mapping[str, object]:
    target = resources.files("meridian_storage.streaming").joinpath(
        "contracts", "public-api", "meridian-streaming.v1.json"
    )
    return cast(Mapping[str, object], json.loads(target.read_text(encoding="utf-8")))


def compatibility_contract() -> Mapping[str, object]:
    target = resources.files("meridian_storage.streaming").joinpath("compatibility.json")
    return cast(Mapping[str, object], json.loads(target.read_text(encoding="utf-8")))


__all__ = ["compatibility_contract", "contract_document", "public_api_contract"]
