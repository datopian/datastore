"""Unit tests for `POST /api/3/datastore_create`.

Covers four scenarios:
    1. valid payload (resource_id and resource branches both succeed)
    2. payload with a missing required field
    3. resource_id not accessible (unknown id and denied api_key)
    4. package not accessible (unknown id and denied api_key)
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

CREATE_URL = "/api/3/action/datastore_create"


def _valid_payload_with_resource_id() -> dict[str, Any]:
    return {
        "resource_id": "balancing_auction_results_2025",
        "fields": [
            {"id": "auction_id", "type": "int4"},
            {"id": "product_code", "type": "text"},
        ],
        "primary_key": ["auction_id", "product_code"],
        "records": [
            {"auction_id": 144, "product_code": "DCL"},
            {"auction_id": 145, "product_code": "DCH"},
        ],
    }


def _valid_payload_with_resource() -> dict[str, Any]:
    return {
        "resource": {
            "id": "balancing_auction_results_2025",
            "name": "Balancing Auction Results 2025",
            "package_id": "pkg-balancing-2025",
        },
        "fields": [
            {"id": "auction_id", "type": "int4"},
            {"id": "product_code", "type": "text"},
        ],
        "primary_key": ["auction_id", "product_code"],
        "records": [],
    }


# 1. Correct payload --------------------------------------------------------


def test_create_with_resource_id_succeeds(client: TestClient) -> None:
    response = client.post(CREATE_URL, json=_valid_payload_with_resource_id())

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    assert result["resource_id"] == "balancing_auction_results_2025"
    assert result["package_id"] == "pkg-balancing-2025"
    # Both surfaces — canonical `schema.primaryKey` and deprecated top-level
    # `primary_key` — carry the same unique key.
    assert result["schema"]["primaryKey"] == ["auction_id", "product_code"]
    assert result["primary_key"] == ["auction_id", "product_code"]
    assert [f["id"] for f in result["fields"]] == ["auction_id", "product_code"]


def test_create_with_resource_dict_succeeds(client: TestClient) -> None:
    response = client.post(CREATE_URL, json=_valid_payload_with_resource())

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    assert result["package_id"] == "pkg-balancing-2025"


# 2. Missing required field -------------------------------------------------


def test_create_missing_fields_and_schema_returns_validation_error(
    client: TestClient,
) -> None:
    """Neither legacy `fields` nor frictionless `schema` provided → 400."""
    payload = _valid_payload_with_resource_id()
    payload.pop("fields")

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    error = body["error"]
    assert error["__type"] == "Validation Error"
    assert "either 'fields' or 'schema' is required" in str(error["fields"])


def test_create_empty_fields_returns_validation_error(client: TestClient) -> None:
    payload = _valid_payload_with_resource_id()
    payload["fields"] = []

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Validation Error"
    assert "'fields' must not be empty" in str(body["error"]["fields"])


def test_create_field_missing_id_returns_validation_error(client: TestClient) -> None:
    payload = _valid_payload_with_resource_id()
    payload["fields"] = [{"type": "int4"}]  # no `id`

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Validation Error"
    assert any("fields[0].id" in path for path in body["error"]["fields"])


# 3. Resource not accessible ------------------------------------------------


def test_create_unknown_resource_id_returns_404(client: TestClient) -> None:
    payload = _valid_payload_with_resource_id()
    payload["resource_id"] = "does-not-exist"

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 404
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Not Found Error"
    assert "does-not-exist" in body["error"]["message"]


def test_create_resource_id_with_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")  # the conftest fixture sets this header

    response = client.post(CREATE_URL, json=_valid_payload_with_resource_id())

    assert response.status_code == 403
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Authorization Error"


# 4. Package not accessible -------------------------------------------------


def test_create_unknown_package_returns_404(client: TestClient) -> None:
    payload = _valid_payload_with_resource()
    payload["resource"]["package_id"] = "missing-package"

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 404
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Not Found Error"
    assert "missing-package" in body["error"]["message"]


def test_create_package_with_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")

    response = client.post(CREATE_URL, json=_valid_payload_with_resource())

    assert response.status_code == 403
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Authorization Error"


# 5. Frictionless `schema` path --------------------------------------------
def _valid_payload_with_schema() -> dict[str, Any]:
    return {
        "resource_id": "balancing_auction_results_2025",
        "schema": {
            "fields": [
                {"name": "auction_id", "type": "integer"},
                {"name": "product_code", "type": "string"},
            ],
            "primaryKey": ["auction_id", "product_code"],
        },
        "records": [
            {"auction_id": 144, "product_code": "DCL"},
        ],
    }


def test_create_with_schema_succeeds_and_returns_both_shapes(
    client: TestClient,
) -> None:
    """Frictionless `schema` input → response carries both `fields` and `schema`."""
    response = client.post(CREATE_URL, json=_valid_payload_with_schema())

    assert response.status_code == 200, response.text
    result = response.json()["result"]
    # Top-level `primary_key` mirrors `schema.primaryKey`.
    assert result["primary_key"] == ["auction_id", "product_code"]
    # Legacy `fields` derived from the schema, with Postgres types.
    assert [f["id"] for f in result["fields"]] == ["auction_id", "product_code"]
    assert result["fields"][0]["type"] == "int8"
    assert result["fields"][1]["type"] == "text"
    # Schema returned verbatim (Frictionless shape).
    assert result["schema"]["primaryKey"] == ["auction_id", "product_code"]
    assert [f["name"] for f in result["schema"]["fields"]] == [
        "auction_id",
        "product_code",
    ]


def test_create_with_fields_returns_both_shapes(client: TestClient) -> None:
    """Legacy `fields` input → response also includes derived frictionless `schema`."""
    response = client.post(CREATE_URL, json=_valid_payload_with_resource_id())

    assert response.status_code == 200, response.text
    result = response.json()["result"]
    schema = result["schema"]
    assert [f["name"] for f in schema["fields"]] == [
        "auction_id",
        "product_code",
    ]
    # int4 → integer, text → string when projecting Postgres → Frictionless.
    assert schema["fields"][0]["type"] == "integer"
    assert schema["fields"][1]["type"] == "string"
    assert schema["primaryKey"] == ["auction_id", "product_code"]


def test_create_with_fields_and_schema_returns_validation_error(
    client: TestClient,
) -> None:
    payload = _valid_payload_with_resource_id()
    payload["schema"] = {"fields": [{"name": "auction_id", "type": "integer"}]}

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["__type"] == "Validation Error"
    assert "not both" in str(error["fields"])


def test_create_with_schema_and_primary_key_returns_validation_error(
    client: TestClient,
) -> None:
    """Top-level `primary_key` is rejected when `schema` is supplied."""
    payload = _valid_payload_with_schema()
    payload["primary_key"] = ["auction_id"]

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["__type"] == "Validation Error"
    assert "primary_key" in str(error["fields"])


def test_create_with_invalid_schema_returns_validation_error(
    client: TestClient,
) -> None:
    """Malformed frictionless schema is rejected at the boundary."""
    payload = _valid_payload_with_schema()
    payload["schema"] = {"fields": [{"name": "auction_id", "type": "not-a-type"}]}

    response = client.post(CREATE_URL, json=payload)

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["__type"] == "Validation Error"
    assert "schema" in error["fields"]
