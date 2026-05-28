from __future__ import annotations

import os

# Neutralise any developer .env before `datastore.main` is imported —
# `create_app()` runs at module load and reads the live process env, so
# fixtures can't intercept BigQuery vars in time. Clearing them here
# keeps the suite hermetic against whatever happens to be in .env.
for _name in (
    "BIGQUERY_PROJECT", "BIGQUERY_DATASET",
    "BIGQUERY_CREDENTIALS", "BIGQUERY_CREDENTIALS_RO",
    "BIGQUERY_EXPORT_BUCKET",
):
    os.environ[_name] = ""
os.environ["BIGQUERY_EXPORT_URL_EXPIRY_HOURS"] = "1"
# `AUTH_TYPE` defaults to `ckan`, whose validator requires a non-empty
# `CKAN_URL`. `create_app()` runs at import (module-level `app`), so give
# it a dummy when the env doesn't carry one — tests override the CKAN
# client via DI, so this URL is never contacted.
os.environ.setdefault("CKAN_URL", "http://test-ckan.local")

from collections.abc import Iterator  # noqa: E402
from typing import Any  # noqa: E402

import pytest  # noqa: E402
from datastore.api.context import get_auth_provider, get_ckan_client  # noqa: E402
from datastore.auth.ckan import Provider as CKANAuthProvider  # noqa: E402
from datastore.core.exceptions import AuthorizationError, NotFoundError  # noqa: E402
from datastore.infrastructure.cache import InMemoryCache  # noqa: E402
from datastore.main import create_app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_bigquery_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the BigQuery engine into placeholder mode for every test.

    The unit suite isn't allowed to contact real BigQuery — engine tests
    mock `client.query` / `client.insert_rows_json` directly, and other
    layers (write service, action endpoints) rely on the backend's
    placeholder-echo branch (active when project/dataset are unset).

    A developer .env that points at a live BQ project would otherwise
    flip the engine into real mode, talk to GCP, and either hang the
    suite on network calls or fail tests that expect echo semantics.
    Clearing the four BQ envs (and resetting the engine cache so a
    previously-built live instance doesn't survive between tests) keeps
    the suite hermetic.
    """
    from datastore.core.config import get_config
    from datastore.infrastructure.engines.registry import reset_engine_cache

    for name in (
        "BIGQUERY_PROJECT", "BIGQUERY_DATASET",
        "BIGQUERY_CREDENTIALS", "BIGQUERY_CREDENTIALS_RO",
        "BIGQUERY_EXPORT_BUCKET",
    ):
        monkeypatch.setenv(name, "")
    # Pydantic-Settings can't parse "" as int — give the dump-URL TTL a
    # valid placeholder so a stray .env doesn't break startup in tests.
    monkeypatch.setenv("BIGQUERY_EXPORT_URL_EXPIRY_HOURS", "1")
    # `Config` and engine instances are lru-cached / module-level
    # singletons; invalidate so the cleared env actually takes effect.
    get_config.cache_clear()
    reset_engine_cache()


class FakeCKAN:
    """In-memory stand-in matching `CKANClient` shape (api_key bound on instance).

    `bind(api_key)` mirrors the real client: returns self with the key set so
    counters and dicts stay shared across the test's single TestClient.
    """

    def __init__(self) -> None:
        self.resources: dict[str, dict[str, Any]] = {}
        self.packages: dict[str, dict[str, Any]] = {}
        self.authorize_calls = 0
        self.deny_keys: set[str] = set()
        self._api_key: str | None = None

    def add_resource(self, resource_id: str, **extra: Any) -> None:
        self.resources[resource_id] = {"id": resource_id, **extra}

    def add_package(self, package_id: str, **extra: Any) -> None:
        self.packages[package_id] = {"id": package_id, **extra}

    def deny(self, api_key: str) -> None:
        self.deny_keys.add(api_key)

    def bind(self, api_key: str | None) -> "FakeCKAN":
        self._api_key = api_key
        return self

    async def datastore_authorize(
        self,
        *,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None = None,
    ) -> dict[str, Any]:
        self.authorize_calls += 1
        if self._api_key and self._api_key in self.deny_keys:
            raise AuthorizationError(f"key '{self._api_key}' is not allowed")

        if resource_id is not None:
            existing = self.resources.get(resource_id)
            if existing is None:
                raise NotFoundError(f"resource '{resource_id}' not found")
            pkg_id = str(existing.get("package_id"))
            package = self.packages.get(pkg_id)
            if package is None:
                raise NotFoundError(f"package '{pkg_id}' not found")
            return {"package": package, "resource": existing}

        assert package_id is not None
        package = self.packages.get(package_id)
        if package is None:
            raise NotFoundError(f"package '{package_id}' not found")
        return {"package": package, "resource": {"package_id": package_id}}

    async def resource_create(self, *, resource: dict[str, Any]) -> dict[str, Any]:
        self._guard()
        package_id = str(resource.get("package_id") or "")
        if package_id not in self.packages:
            raise NotFoundError(f"package '{package_id}' not found")
        created = {**resource}
        created.setdefault("id", str(resource.get("id") or f"res-{len(self.resources) + 1}"))
        self.resources[str(created["id"])] = created
        return created

    async def resource_patch(
        self, *, resource_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        self._guard()
        existing = self.resources.get(resource_id)
        if existing is None:
            raise NotFoundError(f"resource '{resource_id}' not found")
        existing.update(patch)
        return existing

    def _guard(self) -> None:
        if not self._api_key:
            raise AuthorizationError("authentication required")
        if self._api_key in self.deny_keys:
            raise AuthorizationError(f"key '{self._api_key}' is not allowed")


@pytest.fixture
def fake_ckan() -> FakeCKAN:
    ckan = FakeCKAN()
    ckan.add_resource(
        "balancing_auction_results_2025",
        package_id="pkg-balancing-2025",
        name="balancing-auction-results-2025",
        # Seed mirrors a real datastore-managed resource: `url_type` is
        # the marker the read-only-guard checks (see api/auth.py).
        url_type="datastore",
    )
    ckan.add_package(
        "pkg-balancing-2025",
        name="balancing-2025",
        title="Balancing Market 2025",
    )
    return ckan


@pytest.fixture
def cache() -> InMemoryCache:
    return InMemoryCache()


@pytest.fixture
def client(fake_ckan: FakeCKAN, cache: InMemoryCache) -> Iterator[TestClient]:
    app = create_app()
    app.dependency_overrides[get_ckan_client] = lambda: fake_ckan
    # Auth provider talks to the same FakeCKAN — tests don't go through
    # the real HTTP CKAN client. Mirrors what the lifespan would build.
    app.dependency_overrides[get_auth_provider] = lambda: CKANAuthProvider(
        ckan=fake_ckan, cache=cache, cache_ttl=60,
    )
    with TestClient(app) as c:
        c.headers["Authorization"] = "test-token"
        yield c
