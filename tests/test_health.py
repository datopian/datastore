"""End-to-end tests for `GET /`, `GET /health`, `GET /ready`.

Covers:
    1. /          — welcome envelope
    2. /health    — always 200 while the process is up
    3. /ready     — 200 when both engines pass healthcheck; 503 with a
                    Service Unavailable envelope when either fails
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from datastore.infrastructure.engines.registry import (
    reset_engine_cache,
)
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _clean_engine_cache() -> Iterator[None]:
    reset_engine_cache()
    yield
    reset_engine_cache()


# 1. Welcome ----------------------------------------------------------------

def test_welcome_returns_envelope(client: TestClient) -> None:
    response = client.get("/")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert isinstance(body["result"]["message"], str)


def test_welcome_not_mounted_under_action_prefix(client: TestClient) -> None:
    """Welcome is root-only — `/api/3/action/` is the CKAN action
    namespace and shouldn't echo a generic landing message."""
    response = client.get("/api/3/action/")
    assert response.status_code == 404


# 2. /health ----------------------------------------------------------------

def test_health_returns_ok(client: TestClient) -> None:
    """Liveness — always 200 while the process is up."""
    response = client.get("/datastore/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["status"] == "ok"


# 3. /ready -----------------------------------------------------------------

def test_ready_503_when_engine_unhealthy(client: TestClient) -> None:
    """Default test env has `bigquery` engine + no BIGQUERY_PROJECT, so
    the client is never built and healthcheck returns False. Both modes
    fail → 503 in the StatusResponse envelope shape (`result.status` =
    "not_ready"); the HTTP code + `success: false` carry the signal so
    mode names don't leak into the response."""
    response = client.get("/datastore/api/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["result"]["status"] == "not_ready"
    assert "error" not in body


def test_ready_200_when_engines_healthy(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Force every engine instance's healthcheck to True — the same
    pattern other endpoint tests use to swap engine behaviour."""
    from datastore.infrastructure.engines.bigquery.backend import (
        BigQueryBackend,
    )

    monkeypatch.setattr(BigQueryBackend, "healthcheck", lambda self: True)

    response = client.get("/datastore/api/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["status"] == "ready"


def test_ready_503_when_only_rw_fails(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If rw fails but ro passes, /ready still 503s — pod isn't really
    'ready' until both modes are reachable. Envelope stays in
    StatusResponse shape (`result.status` = "not_ready")."""
    from datastore.infrastructure.engines.bigquery.backend import (
        BigQueryBackend,
    )

    def fake_healthcheck(self: BigQueryBackend) -> bool:
        return self.mode == "ro"

    monkeypatch.setattr(BigQueryBackend, "healthcheck", fake_healthcheck)

    response = client.get("/datastore/api/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["result"]["status"] == "not_ready"


def test_ready_handles_engine_construction_error(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If building the engine raises (bad credentials, missing module),
    /ready returns 503 in StatusResponse shape instead of bubbling a 500."""
    def boom(*args: object, **kwargs: object) -> object:
        raise RuntimeError("engine construction failed")

    monkeypatch.setattr(
        "datastore.api.endpoints.health.get_datastore_engine", boom
    )

    response = client.get("/datastore/api/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["result"]["status"] == "not_ready"
