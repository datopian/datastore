from __future__ import annotations

from fastapi.testclient import TestClient


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
