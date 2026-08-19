"""CORS middleware wiring — `CORS_ORIGINS` config → response headers.

`create_app()` reads the lru-cached `Config`, so each test sets the env
var and clears the cache before building its own app. The autouse
`_isolate_bigquery_env` fixture has already cleared the BQ env, so the
lifespan's engine warmup stays in placeholder mode.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from datastore.core.config import get_config
from datastore.main import create_app
from fastapi.testclient import TestClient


@contextmanager
def _client_with_origins(
    monkeypatch: pytest.MonkeyPatch, origins: str
) -> Iterator[TestClient]:
    monkeypatch.setenv("CORS_ORIGINS", origins)
    get_config.cache_clear()
    try:
        with TestClient(create_app()) as client:
            yield client
    finally:
        get_config.cache_clear()


def test_wildcard_allows_any_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client_with_origins(monkeypatch, "*") as client:
        r = client.get("/datastore/api/health", headers={"Origin": "https://anywhere.example"})
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "*"


def test_specific_domain_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    origins = "https://data.example.org, https://app.example.org"
    with _client_with_origins(monkeypatch, origins) as client:
        allowed = client.get(
            "/datastore/api/health", headers={"Origin": "https://app.example.org"}
        )
        denied = client.get(
            "/datastore/api/health", headers={"Origin": "https://evil.example.org"}
        )
    assert allowed.headers["access-control-allow-origin"] == "https://app.example.org"
    assert "access-control-allow-origin" not in denied.headers


def test_preflight_options(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client_with_origins(monkeypatch, "https://data.example.org") as client:
        r = client.options(
            "/datastore/api/v2/datastore_create",
            headers={
                "Origin": "https://data.example.org",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization,content-type",
            },
        )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "https://data.example.org"
    assert "POST" in r.headers["access-control-allow-methods"]


def test_empty_disables_cors(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client_with_origins(monkeypatch, "") as client:
        r = client.get("/datastore/api/health", headers={"Origin": "https://anywhere.example"})
    assert r.status_code == 200
    assert "access-control-allow-origin" not in r.headers
