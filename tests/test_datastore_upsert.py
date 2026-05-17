"""End-to-end tests for `POST /api/3/action/datastore_upsert`.

Covers:
    1. all three methods (upsert, insert, update) and the default method
    2. optional flags — include_records echoes rows, include_total returns
       the count, and both are absent from the body when not requested
    3. records is optional (no-op write)
    4. validation — missing resource_id, invalid method, extra fields
    5. auth — unknown resource_id (404) and denied api_key (403)
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

UPSERT_URL = "/api/3/action/datastore_upsert"

_RESOURCE_ID = "balancing_auction_results_2025"


def _payload(**overrides: Any) -> dict[str, Any]:
    """A valid baseline upsert payload that the fixture's seeded resource accepts."""
    base: dict[str, Any] = {
        "resource_id": _RESOURCE_ID,
        "records": [
            {"auction_id": 144, "product_code": "DCL"},
            {"auction_id": 153, "product_code": "FFR"},
        ],
        "method": "upsert",
    }
    base.update(overrides)
    return base


# 1. Methods -----------------------------------------------------------------

def test_upsert_method_succeeds(client: TestClient) -> None:
    response = client.post(UPSERT_URL, json=_payload(method="upsert"))

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    assert result["resource_id"] == _RESOURCE_ID
    assert result["method"] == "upsert"


def test_insert_method_succeeds(client: TestClient) -> None:
    response = client.post(UPSERT_URL, json=_payload(method="insert"))

    assert response.status_code == 200
    assert response.json()["result"]["method"] == "insert"


def test_update_method_succeeds(client: TestClient) -> None:
    response = client.post(UPSERT_URL, json=_payload(method="update"))

    assert response.status_code == 200
    assert response.json()["result"]["method"] == "update"


def test_default_method_is_upsert(client: TestClient) -> None:
    payload = _payload()
    payload.pop("method")

    response = client.post(UPSERT_URL, json=payload)

    assert response.status_code == 200
    assert response.json()["result"]["method"] == "upsert"


# 2. Optional flags ---------------------------------------------------------

def test_include_records_echoes_records(client: TestClient) -> None:
    payload = _payload(include_records=True)

    response = client.post(UPSERT_URL, json=payload)

    assert response.status_code == 200
    assert response.json()["result"]["records"] == payload["records"]


def test_include_total_returns_total(client: TestClient) -> None:
    response = client.post(UPSERT_URL, json=_payload(include_total=True))

    assert response.status_code == 200
    result = response.json()["result"]
    assert "total" in result
    # BigQuery placeholder returns len(records); real engine will COUNT(*).
    assert result["total"] == 2


def test_default_omits_optional_fields(client: TestClient) -> None:
    """`records` and `total` should not appear when their flags are False."""
    response = client.post(UPSERT_URL, json=_payload())

    assert response.status_code == 200
    result = response.json()["result"]
    assert "records" not in result
    assert "total" not in result


# 3. Records optional --------------------------------------------------------

def test_records_optional(client: TestClient) -> None:
    payload = _payload()
    payload.pop("records")

    response = client.post(UPSERT_URL, json=payload)

    assert response.status_code == 200
    assert response.json()["success"] is True


# 4. Validation --------------------------------------------------------------

def test_missing_resource_id_returns_validation_error(client: TestClient) -> None:
    payload = _payload()
    payload.pop("resource_id")

    response = client.post(UPSERT_URL, json=payload)

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Validation Error"
    assert "resource_id" in body["error"]["fields"]


def test_invalid_method_returns_validation_error(client: TestClient) -> None:
    response = client.post(UPSERT_URL, json=_payload(method="delete"))

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "method" in body["error"]["fields"]


def test_extra_field_rejected(client: TestClient) -> None:
    """`extra='forbid'` on the schema should reject unknown keys."""
    response = client.post(UPSERT_URL, json=_payload(unknown_key="oops"))

    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


# 5. Auth --------------------------------------------------------------------

def test_unknown_resource_id_returns_404(client: TestClient) -> None:
    response = client.post(
        UPSERT_URL, json=_payload(resource_id="does-not-exist")
    )

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["__type"] == "Not Found Error"
    assert "does-not-exist" in body["error"]["message"]


def test_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")  # conftest sets this header on the client

    response = client.post(UPSERT_URL, json=_payload())

    assert response.status_code == 403
    body = response.json()
    assert body["error"]["__type"] == "Authorization Error"
