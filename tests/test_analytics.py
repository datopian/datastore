"""One structured analytics event per datastore action call or dump.

The middleware is exercised through the real app - the same routes, error
handlers and auth the service runs - with the emitter captured. The event
shape is shared with ckanext-analytics: both emit JSON log lines meant for
one BigQuery table, told apart by `service`.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import patch

import pytest
from datastore import analytics
from datastore.api.context import RequestContext, get_auth_provider, get_ckan_client
from datastore.auth.base import Decision
from datastore.auth.ckan import Provider as CKANAuthProvider
from datastore.core.config import get_config
from datastore.infrastructure.cache import InMemoryCache
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from datastore.main import create_app
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

FIELDS = {
    "timestamp",
    "request_id",
    "service",
    "method",
    "endpoint",
    "query_string",
    "action_type",
    "status_code",
    "user_agent",
    "request_ip",
    "user",
    "dataset",
    "resource",
    "organization",
    "group",
}

SEARCH_URL = "/api/3/action/datastore_search"
RESOURCE = "balancing_auction_results_2025"


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    events: list[dict] = []
    monkeypatch.setattr(analytics, "emit_event", events.append)
    return events


# --- what gets recorded -----------------------------------------------------


def test_a_search_is_recorded_with_the_whole_field_set(
    client: TestClient, recorded: list[dict]
) -> None:
    response = client.get(SEARCH_URL, params={"resource_id": RESOURCE})

    assert response.status_code == 200
    assert len(recorded) == 1
    event = recorded[0]
    assert set(event) == FIELDS
    assert event["service"] == "Datastore"
    assert event["method"] == "GET"
    assert event["endpoint"] == SEARCH_URL
    assert event["query_string"] == f"resource_id={RESOURCE}"
    assert event["action_type"] == "datastore_search"
    assert event["status_code"] == 200


def test_the_resource_is_resolved_through_the_auth_decision(
    client: TestClient, recorded: list[dict]
) -> None:
    """Authorize already fetched the CKAN names - the event reuses them."""
    client.get(SEARCH_URL, params={"resource_id": RESOURCE})

    event = recorded[0]
    assert event["resource"] == "balancing-auction-results-2025"
    assert event["dataset"] == "balancing-2025"


def test_the_caller_is_recorded_by_username(
    client: TestClient, recorded: list[dict]
) -> None:
    """CKAN's datastore_authorize names the acting user; the event keeps it."""
    client.get(SEARCH_URL, params={"resource_id": RESOURCE})

    assert recorded[0]["user"] == "jhon"


def test_a_post_carries_its_resource_in_the_body(
    client: TestClient, recorded: list[dict]
) -> None:
    """nginx cannot see a POST body; this is why the service records itself."""
    client.post(
        "/api/3/action/datastore_upsert",
        json={"resource_id": RESOURCE, "force": True, "records": [{"a": 1}]},
    )

    event = recorded[0]
    assert event["action_type"] == "datastore_upsert"
    assert event["method"] == "POST"
    assert event["resource"] == "balancing-auction-results-2025"


def test_a_dump_is_recorded_as_a_download(
    client: TestClient, recorded: list[dict]
) -> None:
    url = "https://storage.googleapis.com/bucket/dumps/x/abc.csv?Sig=abc"

    async def fake_dump(self: BigQueryBackend, resource_id: str, fmt: str) -> list[str]:
        return [url]

    with patch.object(BigQueryBackend, "dump", fake_dump):
        response = client.get(f"/datastore/dump/{RESOURCE}", follow_redirects=False)

    assert response.status_code == 302
    event = recorded[0]
    assert event["action_type"] == "datastore_dump"
    assert event["status_code"] == 302
    assert event["resource"] == "balancing-auction-results-2025"


def test_a_sql_dump_is_recorded_under_its_own_name(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get("/datastore/dump/query", params={"sql": "SELECT 1"})

    assert recorded[0]["action_type"] == "datastore_dump_query"


# --- failures are half the data ----------------------------------------------


def test_a_denied_call_is_recorded_with_the_raw_reference(
    client: TestClient, recorded: list[dict], fake_ckan: FakeCKAN
) -> None:
    """No names were resolved, so the event keeps what the caller sent."""
    fake_ckan.deny("bad-key")

    response = client.get(
        SEARCH_URL,
        params={"resource_id": RESOURCE},
        headers={"Authorization": "bad-key"},
    )

    assert response.status_code == 403
    event = recorded[0]
    assert event["status_code"] == 403
    assert event["resource"] == RESOURCE


def test_a_missing_resource_is_recorded_with_its_status(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get(SEARCH_URL, params={"resource_id": "no-such-resource"})

    assert recorded[0]["status_code"] == 404
    assert recorded[0]["resource"] == "no-such-resource"


def test_an_unmounted_action_is_recorded_as_its_status(
    client: TestClient, recorded: list[dict]
) -> None:
    """This service only mounts datastore actions; attempts still count."""
    client.get("/api/3/action/package_show")

    assert recorded[0]["action_type"] == "package_show"
    assert recorded[0]["status_code"] == 404


# --- request metadata ---------------------------------------------------------


def test_request_metadata_comes_from_the_ingress_headers(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get(
        SEARCH_URL,
        params={"resource_id": RESOURCE},
        headers={
            "X-Request-ID": "abc123-from-nginx",
            "X-Real-IP": "203.0.113.7",
            "User-Agent": "curl/8.4.0",
        },
    )

    event = recorded[0]
    assert event["request_id"] == "abc123-from-nginx"
    assert event["request_ip"] == "203.0.113.7"
    assert event["user_agent"] == "curl/8.4.0"


def test_a_request_id_is_generated_when_nothing_upstream_set_one(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get(SEARCH_URL, params={"resource_id": RESOURCE})

    assert recorded[0]["request_id"]


def test_the_ip_falls_back_to_the_last_forwarded_for_entry(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get(
        SEARCH_URL,
        params={"resource_id": RESOURCE},
        headers={"X-Forwarded-For": "10.0.0.9, 203.0.113.7"},
    )

    assert recorded[0]["request_ip"] == "203.0.113.7"


# --- what does not get recorded, and what cannot break ------------------------


def test_health_and_pages_are_not_recorded(
    client: TestClient, recorded: list[dict]
) -> None:
    client.get("/")
    client.get("/datastore/api/health")

    assert recorded == []


def test_a_broken_emitter_does_not_break_the_request(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def explode(event: dict) -> None:
        raise RuntimeError("the stream is down")

    monkeypatch.setattr(analytics, "emit_event", explode)

    response = client.get(SEARCH_URL, params={"resource_id": RESOURCE})

    assert response.status_code == 200


def test_analytics_can_be_disabled_by_env(
    fake_ckan: FakeCKAN,
    cache: InMemoryCache,
    recorded: list[dict],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ANALYTICS_ENABLED=false leaves the middleware unmounted — requests
    work as ever, no event is emitted."""
    monkeypatch.setenv("ANALYTICS_ENABLED", "false")
    get_config.cache_clear()

    app = create_app()
    app.dependency_overrides[get_ckan_client] = lambda: fake_ckan
    app.dependency_overrides[get_auth_provider] = lambda: CKANAuthProvider(
        ckan=fake_ckan, cache=cache, cache_ttl=60,
    )
    with TestClient(app) as c:
        c.headers["Authorization"] = "test-token"
        response = c.get(SEARCH_URL, params={"resource_id": RESOURCE})

    assert response.status_code == 200
    assert recorded == []


def test_authorize_captures_nothing_when_analytics_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the middleware unmounted nobody reads the attribution -
    authorize should not stash it on the request either."""
    monkeypatch.setenv("ANALYTICS_ENABLED", "false")
    get_config.cache_clear()

    class StubRequest:
        def __init__(self) -> None:
            self.scope: dict[str, Any] = {}

    class StubProvider:
        name = "stub"

        async def authorize(self, **_: object) -> Decision:
            return Decision(subject="jhon", resource={"id": "r"}, package={"id": "p"})

        def key_id(self, credential: str) -> str:
            return "h:stub"

    request = StubRequest()
    context = RequestContext(
        config=get_config(),
        api_key="tok",
        auth_provider=StubProvider(),
        ckan=None,
        request=request,  # type: ignore[arg-type]
    )
    asyncio.run(context.authorize(resource_id="r", permission="read"))

    assert request.scope.get("state", {}).get("analytics") is None


def test_the_emitted_line_is_bare_json(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing downstream can parse the event unless the line is only the object."""
    with caplog.at_level("INFO", logger="datastore.analytics"):
        analytics.emit_event({"action_type": "datastore_search", "status_code": 200})

    assert json.loads(caplog.records[0].getMessage()) == {
        "action_type": "datastore_search",
        "status_code": 200,
    }
