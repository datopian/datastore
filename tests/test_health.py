"""End-to-end tests for `GET /`, `GET /health`, `GET /ready`.

Covers:
    1. /          — welcome envelope
    2. /health    — always 200 while the process is up
    3. /ready     — 200 when both engines pass healthcheck; 503 with a
                    Service Unavailable envelope when either fails
"""

from __future__ import annotations

import pytest
from datastore.infrastructure.engines.registry import (
    reset_engine_cache,
)
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _clean_engine_cache():
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


# 2. /health ----------------------------------------------------------------

def test_health_returns_ok(client: TestClient) -> None:
    """Liveness — always 200."""
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["status"] == "ok"


# 3. /ready -----------------------------------------------------------------

def test_ready_503_when_engine_unhealthy(client: TestClient) -> None:
    """Default test env has `bigquery` engine + no BIGQUERY_PROJECT, so
    the client is never built and healthcheck returns False. Both modes
    fail → 503 with a Service Unavailable envelope listing them."""
    response = client.get("/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Service Unavailable"
    msg = body["error"]["message"]
    assert "rw" in msg and "ro" in msg


def test_ready_200_when_engines_healthy(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Force every engine instance's healthcheck to True — the same
    pattern other endpoint tests use to swap engine behaviour."""
    from datastore.infrastructure.engines.bigquery.backend import (
        BigQueryBackend,
    )

    monkeypatch.setattr(BigQueryBackend, "healthcheck", lambda self: True)

    response = client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["status"] == "ready"


def test_ready_503_when_only_rw_fails(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If rw fails but ro passes, /ready still 503s — pod isn't really
    'ready' until both modes are reachable."""
    from datastore.infrastructure.engines.bigquery.backend import (
        BigQueryBackend,
    )

    def fake_healthcheck(self):
        return self.mode == "ro"

    monkeypatch.setattr(BigQueryBackend, "healthcheck", fake_healthcheck)

    response = client.get("/ready")

    assert response.status_code == 503
    msg = response.json()["error"]["message"]
    assert "rw" in msg
    assert "ro" not in msg


def test_ready_handles_engine_construction_error(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If building the engine raises (bad credentials, missing module),
    /ready marks that mode failing instead of bubbling a 500."""
    def boom(*args, **kwargs):
        raise RuntimeError("engine construction failed")

    monkeypatch.setattr(
        "datastore.api.endpoints.health.get_datastore_engine", boom
    )

    response = client.get("/ready")

    assert response.status_code == 503
    msg = response.json()["error"]["message"]
    assert "rw" in msg and "ro" in msg
