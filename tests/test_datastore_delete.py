"""End-to-end tests for `POST /api/3/action/datastore_delete`.

Body accepts:
    resource_id / id (one required) — table to delete from
    filters (optional dict)         — narrow the delete; omit → drop table
    force   (optional bool)         — required for read-only resources

Response echoes back the original `filters` (CKAN convention) under
`result.{resource_id, filters}`.

Covers:
    1. happy path — filtered + whole-table drop
    2. aliases — `id` accepted, normalised to `resource_id`
    3. validation — missing both, unknown body keys
    4. auth — unknown resource (404), denied key (403)

Placeholder engine doesn't actually delete anything; these tests pin
shape + routing.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

DELETE_URL = "/api/3/action/datastore_delete"
_RESOURCE_ID = "balancing_auction_results_2025"


# 1. Happy path -------------------------------------------------------------

def test_delete_with_filters_echoes_them(client: TestClient) -> None:
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "filters": {"product_code": "DCL", "accepted": False},
    })

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["resource_id"] == _RESOURCE_ID
    assert body["result"]["filters"] == {
        "product_code": "DCL", "accepted": False,
    }


def test_delete_without_filters_drops_whole_table(client: TestClient) -> None:
    """Omitting `filters` means drop the entire table; the response
    omits `filters` from `result` (exclude_none)."""
    response = client.post(DELETE_URL, json={"resource_id": _RESOURCE_ID})

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["resource_id"] == _RESOURCE_ID
    assert "filters" not in result


def test_force_flag_accepted(client: TestClient) -> None:
    """`force=True` is accepted (the placeholder doesn't enforce
    read-only; real BigQuery impl will check resource metadata)."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "filters": {"x": 1},
        "force": True,
    })
    assert response.status_code == 200


# 2. Aliases ----------------------------------------------------------------

def test_id_alias_works(client: TestClient) -> None:
    """`id` is normalised to `resource_id` by the schema validator."""
    response = client.post(DELETE_URL, json={"id": _RESOURCE_ID})

    assert response.status_code == 200
    assert response.json()["result"]["resource_id"] == _RESOURCE_ID


def test_same_value_for_resource_id_and_id_accepted(client: TestClient) -> None:
    """Same value on both keys is the no-conflict legacy-echo case."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "id": _RESOURCE_ID,
    })
    assert response.status_code == 200
    assert response.json()["result"]["resource_id"] == _RESOURCE_ID


def test_conflicting_resource_id_and_id_rejected(client: TestClient) -> None:
    """Different `resource_id` vs `id` → 400. Silently preferring one
    would let a typo destroy the wrong resource."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "id": "different-value",
    })
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


# 3. Validation -------------------------------------------------------------

def test_missing_both_returns_validation_error(client: TestClient) -> None:
    response = client.post(DELETE_URL, json={})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


def test_extra_body_key_rejected(client: TestClient) -> None:
    """`extra='forbid'` blocks unknown keys to catch typos."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "filterz": {"x": 1},  # typo
    })

    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


def test_filters_and_fields_are_mutually_exclusive(client: TestClient) -> None:
    """Row delete (`filters`) and column drop (`fields`) are separate
    operations; sending both is ambiguous and rejected up front."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "filters": {"id": 1},
        "fields": ["label"],
    })

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "mutually exclusive" in body["error"]["message"]


def test_empty_fields_list_rejected(client: TestClient) -> None:
    """`fields=[]` is ambiguous (column drop with no columns) — 400
    rather than silently no-op."""
    response = client.post(DELETE_URL, json={
        "resource_id": _RESOURCE_ID,
        "fields": [],
    })

    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


# 4. Auth -------------------------------------------------------------------

def test_unknown_resource_returns_404(client: TestClient) -> None:
    response = client.post(DELETE_URL, json={"resource_id": "does-not-exist"})

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["__type"] == "Not Found Error"


def test_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")

    response = client.post(DELETE_URL, json={"resource_id": _RESOURCE_ID})

    assert response.status_code == 403
    assert response.json()["error"]["__type"] == "Authorization Error"


# 5. Read-only resource guard (url_type="datastore") ------------------------


def test_delete_on_readonly_resource_requires_force(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.add_resource(
        "ro-res", package_id="pkg-balancing-2025", url_type="datastore"
    )

    response = client.post(DELETE_URL, json={"resource_id": "ro-res"})

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["__type"] == "Validation Error"
    assert "read-only" in error["message"]


def test_delete_on_readonly_resource_with_force_succeeds(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.add_resource(
        "ro-res", package_id="pkg-balancing-2025", url_type="datastore"
    )

    response = client.post(
        DELETE_URL, json={"resource_id": "ro-res", "force": True}
    )

    assert response.status_code == 200
    assert response.json()["success"] is True
