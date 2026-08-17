"""One structured event per datastore action call or dump.

The event shape is shared with ckanext-analytics: both services emit one
JSON line per request (here on this module's ``datastore.analytics`` logger), all
meant for one BigQuery table (see that repo's ``bigquery.sql``), told apart
by the ``service`` field. Whatever ships the logs (Loki, a GCP log sink, a
file) is what carries events downstream. What the ingress log cannot see is
what this records - the resource a POST body names, who called it, and the
dataset and organization the resource belongs to.

Two pieces:

``AnalyticsMiddleware``
    Pure ASGI, so handled error responses are recorded with their status and
    an unhandled crash is recorded as a 500 before it propagates. Tracks
    ``/api/3/action/*`` and ``/datastore/dump/*``; probes, docs and the
    welcome page are excluded by definition.

``authorization_dict``
    Called by ``RequestContext.authorize`` with the authorized data_dict.
    The CKAN auth provider already fetched (and cached) the user, resource
    and package on the way to its verdict, so the caller's name and the
    resolved dataset / organization names cost the event nothing. Under jwt
    or anonymous auth only the JWT subject (if any) and the raw reference
    are recorded.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

log = logging.getLogger(__name__)

ACTION_PREFIX = "/api/3/action/"
DUMP_PREFIX = "/datastore/dump/"


def action_name(path: str) -> str | None:
    """What to call the event for this path, or None if it is not tracked."""
    if path.startswith(ACTION_PREFIX):
        name = path[len(ACTION_PREFIX):].strip("/").split("/", 1)[0]
        return name or None
    if path == "/datastore/dump/query":
        return "datastore_dump_query"
    if path.startswith(DUMP_PREFIX):
        return "datastore_dump"
    return None


def authorization_dict(request: Any, data_dict: dict[str, Any]) -> None:
    """Keep what authorization learned, for the event this request becomes.

    Lives on ``scope["state"]``, which the middleware and the endpoint's
    ``Request`` share. First answer wins per field: ``datastore_search_sql``
    authorizes several resources, and the event is attributed to the first.
    """
    state: dict[str, Any] = request.scope.setdefault("state", {})
    info: dict[str, Any] = state.setdefault("analytics", {})
    resource = data_dict.get("resource") or {}
    package = data_dict.get("package") or {}
    organization = package.get("organization") or {}
    for key, value in (
        ("user", data_dict.get("user")),
        ("resource", resource.get("name") or resource.get("id")),
        ("dataset", package.get("name") or package.get("id")),
        ("organization", organization.get("name")),
    ):
        if value and info.get(key) is None:
            info[key] = value


class AnalyticsMiddleware:
    """Record every tracked request, whatever became of it."""

    METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})

    #: POST bodies are read only this far - enough for any reference, and a
    #: bound on what a bulk upsert costs to copy.
    BODY_CAP = 64 * 1024

    REQUEST_ID_HEADER = "x-request-id"
    REAL_IP_HEADER = "x-real-ip"
    FORWARDED_FOR_HEADER = "x-forwarded-for"

    def __init__(self, app: ASGIApp, service: str = "datastore-api") -> None:
        self.app = app
        self.service = service

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] not in self.METHODS:
            return await self.app(scope, receive, send)
        action = action_name(scope["path"])
        if action is None:
            return await self.app(scope, receive, send)

        # Created here if authorize has not run yet, so both sides mutate the
        # one dict Starlette's `Request.state` also uses.
        state: dict[str, Any] = scope.setdefault("state", {})
        status = 500  # what reaches the caller if the app dies before a response
        body = bytearray()
        capture = scope["method"] == "POST" and scope["path"].startswith(ACTION_PREFIX)

        async def tee_receive() -> Any:
            message = await receive()
            if capture and message["type"] == "http.request" and len(body) < self.BODY_CAP:
                body.extend(message.get("body", b"")[: self.BODY_CAP - len(body)])
            return message

        async def watch_send(message: Any) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, tee_receive, watch_send)
        finally:
            self._record(scope, action, status, bytes(body), state.get("analytics") or {})

    def _record(
        self,
        scope: Scope,
        action: str,
        status: int,
        body: bytes,
        resolved: dict[str, Any],
    ) -> None:
        """Build and emit the event. A listener that raises would become the
        request's problem, so it may not."""
        try:
            emit_event(self._event(scope, action, status, body, resolved))
        except Exception:
            log.exception("analytics: could not record request")

    def _event(
        self,
        scope: Scope,
        action: str,
        status: int,
        body: bytes,
        resolved: dict[str, Any],
    ) -> dict[str, Any]:
        headers = Headers(scope=scope)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": headers.get(self.REQUEST_ID_HEADER) or uuid.uuid4().hex,
            "service": self.service,
            "method": scope["method"],
            "endpoint": scope["path"],
            "query_string": scope.get("query_string", b"").decode("latin-1") or None,
            "action_type": action,
            "status_code": status,
            "user_agent": headers.get("user-agent") or None,
            "request_ip": self._request_ip(scope, headers),
            "user": resolved.get("user"),
            "dataset": resolved.get("dataset"),
            "resource": resolved.get("resource") or self._resource_ref(scope, body),
            "organization": resolved.get("organization"),
            "group": None,
        }

    def _request_ip(self, scope: Scope, headers: Headers) -> str | None:
        """The caller's address, as the ingress reports it - the same rules
        as ckanext-analytics, and like there, fine for analytics only."""
        real_ip = headers.get(self.REAL_IP_HEADER)
        if real_ip:
            return real_ip.strip() or None

        forwarded = headers.get(self.FORWARDED_FOR_HEADER)
        if forwarded:
            return forwarded.rsplit(",", 1)[-1].strip() or None

        client = scope.get("client")
        return client[0] if client else None

    def _resource_ref(self, scope: Scope, body: bytes) -> str | None:
        """The resource reference the caller sent, wherever they put it.

        The dump URL holds it as a path segment, a GET in the query string,
        a POST in the JSON body. ``datastore_search_sql`` carries its
        resources inside the SQL text, which only authorization can name -
        those events rely on ``authorization_dict`` alone.
        """
        path: str = scope["path"]
        if path.startswith(DUMP_PREFIX):
            ref = path[len(DUMP_PREFIX):].split("/", 1)[0]
            return ref if ref and ref != "query" else None

        query: bytes = scope.get("query_string", b"")
        params = parse_qs(query.decode("latin-1"))
        if params.get("resource_id"):
            return params["resource_id"][0]

        if body:
            try:
                parsed = json.loads(body)
            except ValueError:
                return None
            if isinstance(parsed, dict):
                sent = parsed.get("resource_id") or parsed.get("id")
                return sent if isinstance(sent, str) else None
        return None


def emit_event(event: dict[str, Any]) -> None:
    """Where an event goes: one bare JSON line on the event logger.

    The line is the complete record - whatever ships the logs is what carries
    events downstream. ckanext-analytics' ``bigquery.sql`` documents the table
    they are meant to land in.
    """
    log.info(json.dumps(event, separators=(",", ":"), default=str))
