"""End-to-end tests for `GET /api/3/action/datastore_info`.

Single `resource_id` query parameter; the response envelope's `result`
holds `meta` (free-form dict) + `fields` (column schema list).

Covers:
    1. happy path — known resource_id returns 200 with envelope
    2. validation — missing resource_id, unknown query params
    3. auth — unknown resource_id (404), denied api_key (403)

The placeholder BigQuery engine returns an empty fields list and a small
meta dict, so these tests pin the request/response shape and routing.
Engine-specific content lives with the real backend.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

INFO_URL = "/api/3/action/datastore_info"
_RESOURCE_ID = "balancing_auction_results_2025"


# 1. Happy path -------------------------------------------------------------

def test_basic_info_succeeds(client: TestClient) -> None:
    response = client.get(INFO_URL, params={"resource_id": _RESOURCE_ID})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    # Placeholder engine returns empty fields + a small meta echoing
    # the requested resource_id.
    assert "fields" in result
    assert "meta" in result
    assert isinstance(result["fields"], list)
    assert isinstance(result["meta"], dict)
    assert result["meta"]["resource_id"] == _RESOURCE_ID


def test_response_shape(client: TestClient) -> None:
    """`result` keys are exactly `meta` + `fields` (plus the envelope's
    `help` + `success` at the top level)."""
    response = client.get(INFO_URL, params={"resource_id": _RESOURCE_ID})

    body = response.json()
    assert set(body) == {"help", "success", "result"}
    assert set(body["result"]) == {"meta", "fields"}


# 2. Validation + aliases ---------------------------------------------------

def test_id_alias_works(client: TestClient) -> None:
    """`id` is a CKAN-style alias for `resource_id`; either is accepted."""
    response = client.get(INFO_URL, params={"id": _RESOURCE_ID})

    assert response.status_code == 200
    result = response.json()["result"]
    # Placeholder engine's meta echoes the normalised resource_id.
    assert result["meta"]["resource_id"] == _RESOURCE_ID


def test_resource_id_wins_when_both_provided(client: TestClient) -> None:
    """If both `resource_id` and `id` are present, `resource_id` is used."""
    response = client.get(INFO_URL, params={
        "resource_id": _RESOURCE_ID,
        "id": "ignored-value",
    })

    assert response.status_code == 200
    assert response.json()["result"]["meta"]["resource_id"] == _RESOURCE_ID


def test_missing_both_returns_validation_error(client: TestClient) -> None:
    """Neither `resource_id` nor `id` → 400."""
    response = client.get(INFO_URL, params={})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


def test_extra_query_param_rejected(client: TestClient) -> None:
    """`extra='forbid'` — only `resource_id` / `id` are allowed."""
    response = client.get(INFO_URL, params={
        "resource_id": _RESOURCE_ID,
        "verbose": "true",
    })

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


# 3. Auth -------------------------------------------------------------------

def test_unknown_resource_returns_404(client: TestClient) -> None:
    response = client.get(INFO_URL, params={"resource_id": "does-not-exist"})

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["__type"] == "Not Found Error"
    assert "does-not-exist" in body["error"]["message"]


def test_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")

    response = client.get(INFO_URL, params={"resource_id": _RESOURCE_ID})

    assert response.status_code == 403
    assert response.json()["error"]["__type"] == "Authorization Error"
