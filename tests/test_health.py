"""End-to-end tests for the health probes.

Covers:
    1. /          — no route; the service has no landing endpoint
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


# 1. No landing endpoint ----------------------------------------------------


def test_root_is_not_routed(client: TestClient) -> None:
    """There is no welcome/landing endpoint — every route lives under the
    versioned API prefix."""
    assert client.get("/").status_code == 404


def test_action_prefix_root_is_not_routed(client: TestClient) -> None:
    """The action namespace itself isn't a route either."""
    assert client.get("/datastore/api/v2/").status_code == 404


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


def test_ready_503_when_only_rw_fails(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr("datastore.api.endpoints.health.get_datastore_engine", boom)

    response = client.get("/datastore/api/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["result"]["status"] == "not_ready"
