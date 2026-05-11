from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

DATASTORE_ENDPOINTS = [
    "/api/3/datastore_create",
    "/api/3/datastore_search",
    "/api/3/datastore_upsert",
    "/api/3/datastore_search_sql",
    "/api/3/datastore_delete",
    "/api/3/datastore_info",
]


def test_welcome(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert "message" in body and body["message"]


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_stub(client: TestClient) -> None:
    response = client.get("/ready")
    assert response.status_code == 200


def test_openapi_loads(client: TestClient) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()["info"]["title"]


@pytest.mark.parametrize(
    "path",
    ["/api/3/datastore_create", "/api/3/datastore_upsert", "/api/3/datastore_delete"],
)
def test_datastore_post_echo(client: TestClient, path: str) -> None:
    response = client.post(path, json={"resource_id": "balancing_auction_results_2025"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["success"] is True
    assert "help" in body
    assert body["result"]["resource_id"] == "balancing_auction_results_2025"


@pytest.mark.parametrize("path", ["/api/3/datastore_search", "/api/3/datastore_info"])
def test_datastore_get_echo(client: TestClient, path: str) -> None:
    response = client.get(path, params={"resource_id": "balancing_auction_results_2025"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["success"] is True
    assert "help" in body


def test_datastore_search_sql_echo(client: TestClient) -> None:
    response = client.get("/api/3/datastore_search_sql", params={"sql": "SELECT 1"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["records"] == []


def test_openapi_exposes_datastore_endpoints(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    for endpoint in DATASTORE_ENDPOINTS:
        assert endpoint in paths, f"missing {endpoint} in OpenAPI"
    for endpoint in ["/", "/health", "/ready"]:
        assert endpoint in paths
