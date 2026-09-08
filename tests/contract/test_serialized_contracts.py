# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from meridian_storage.streaming import (
    Cursor,
    Delivery,
    GroupPositionTransition,
    PublishReceipt,
    RangePage,
    ReplayOperation,
    Stream,
    compatibility_contract,
    contract_document,
    public_api_contract,
)

CONTRACT_ROOT = (
    Path(__file__).resolve().parents[2] / "src" / "meridian_storage" / "streaming" / "contracts"
)


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.contract
def test_all_json_schemas_are_valid() -> None:
    for path in sorted((CONTRACT_ROOT / "streaming").glob("*.schema.json")):
        Draft202012Validator.check_schema(_load(path))


@pytest.mark.contract
def test_model_documents_validate_against_released_schemas(
    event,
    position,
    delivery_token,
    stream_ref,
    schema_ref,
) -> None:
    checker = FormatChecker()
    data_schema = _load(CONTRACT_ROOT / "streaming" / "meridian.streaming.data.v1.schema.json")
    resource_schema = _load(
        CONTRACT_ROOT / "streaming" / "meridian.streaming.resource.v1.schema.json"
    )
    operation_schema = _load(
        CONTRACT_ROOT / "streaming" / "meridian.streaming.explicit-operation.v1.schema.json"
    )
    delivery = Delivery(
        event,
        delivery_token,
        position,
        1,
        False,
        "2026-08-25T12:00:01Z",
    )
    documents = (
        Cursor("opaque", stream_ref).to_dict(),
        position.to_dict(),
        delivery_token.to_dict(),
        event.to_dict(),
        delivery.to_dict(),
        PublishReceipt("event-1", position, None, "2026-08-25T12:00:01Z").to_dict(),
        RangePage((event,), Cursor("next", stream_ref), False).to_dict(),
    )
    validator = Draft202012Validator(data_schema, format_checker=checker)
    for document in documents:
        validator.validate(document)
    Draft202012Validator(resource_schema, format_checker=checker).validate(
        Stream(stream_ref, schema_ref).to_dict()
    )
    explicit = Draft202012Validator(operation_schema, format_checker=checker)
    explicit.validate(
        ReplayOperation(stream_ref, Cursor("start", stream_ref)).to_operation().to_dict()
    )
    explicit.validate(
        GroupPositionTransition(
            "orders.subscription",
            "orders.workers",
            "opaque",
            f"sha256:{'a' * 64}",
            "policy",
            "recovery",
        )
        .to_operation()
        .to_dict()
    )


@pytest.mark.contract
def test_public_contract_and_validation_recipe_are_versioned() -> None:
    public = public_api_contract()
    compatibility = compatibility_contract()
    assert public["formatVersion"] == "meridian-streaming-public-api.v1"
    assert public["expressionMethods"] == [
        "acknowledge",
        "create_resource",
        "negative_acknowledge",
        "poll",
        "publish",
        "publish_batch",
        "publish_schema",
        "read_range",
        "subscribe",
    ]
    assert compatibility["design"] == {"catalogsRevision": 70, "hldRevision": 56}
    assert compatibility["core"]["version"] == "1.1.0"  # type: ignore[index]
    assert compatibility["semantics"]["version"] == "2.0.1"  # type: ignore[index]
    assert contract_document("meridian.streaming.data.v1.schema.json")["title"]
    with pytest.raises(ValueError):
        contract_document("../compatibility.json")
    with pytest.raises(FileNotFoundError):
        contract_document("unknown.json")
