"""Swagger UI page + OpenAPI schema shaping.

Everything that decides how this service documents itself: the vendored
Swagger UI route, and the post-processing that tailors the generated schema
(auth scheme per `AUTH_TYPE`, dropping FastAPI's phantom 422).

Also holds the prose it places into the schema — the description, the
per-`AUTH_TYPE` authentication paragraph, and the tag blurbs — so anything
that shapes the docs page is in one file.

Lives in `api/` rather than `core/` because most of it takes a `FastAPI`
instance, and `core/` must stay framework-free.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.responses import HTMLResponse

from datastore.core.config import Config
from datastore.core.constants import API_PREFIX, API_VERSION

# Swagger UI is vendored under `api/static/` rather than pulled from a CDN,
# so the docs page renders in air-gapped deployments and can't break
# when an upstream CDN moves. Version pinned in
# `api/static/swagger-ui/VERSION.txt`.
_STATIC_DIR = Path(__file__).resolve().parent / "static"
_STATIC_MOUNT = f"{API_PREFIX}/static"


# Jinja renders the page (see `api/templates/docs.html`) rather than an
# f-string, so autoescaping is structural and the HTML lives in an .html file.
# `autoescape=True` covers the HTML contexts only — the template's `<style>`
# and `<script>` blocks rely on `Config`'s CSS-colour validator and Jinja's
# `tojson` filter respectively.
_TEMPLATES = Jinja2Templates(directory=Path(__file__).resolve().parent / "templates")


def register_swagger_docs(app: FastAPI, docs_url: str, config: Config) -> None:
    """Mount the vendored assets and serve the themed Swagger UI page.

    Also stamps `info.version` with the API *contract* version. Set here
    rather than on the `FastAPI(...)` constructor so the app factory stays
    free of docs concerns — and because the value is a property of the
    documented surface, not of the running application. FastAPI would
    otherwise leave its hardcoded `0.1.0` default in the schema.
    """
    app.version = API_VERSION

    app.mount(
        _STATIC_MOUNT,
        StaticFiles(directory=_STATIC_DIR),
        name="docs-static",
    )

    @app.get(docs_url, include_in_schema=False)
    async def swagger_ui_html(request: Request) -> HTMLResponse:
        root_path = request.scope.get("root_path", "").rstrip("/")
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="docs.html",
            context={
                "spec_url": root_path + str(app.openapi_url),
                "static_url": root_path + _STATIC_MOUNT,
                "page_title": f"{app.title} - Swagger UI",
                "site_title": config.DOCS_SITE_TITLE or app.title,
                "api_version": app.version,
                "primary_color": config.DOCS_PRIMARY_COLOR,
                "header_color": config.DOCS_HEADER_COLOR,
                "logo_url": config.DOCS_LOGO_URL,
            },
        )


# Swagger "Authorize" description per built-in AUTH_TYPE. A provider not
# listed here (a third-party drop-in under `datastore/auth/<name>/`) keeps
# the generic description declared on the scheme in `api/context.py`.
_AUTH_SCHEME_DESCRIPTIONS = {
    "ckan": (
        "Paste your API token below. It is sent as-is in the "
        "`Authorization` header, with no `Bearer` prefix."
    ),
    "jwt": "Paste signed JWT. Accepts the raw token or `Bearer <token>`.",
}


def tailor_auth_scheme(app: FastAPI, auth_type: str) -> None:
    """Shape the OpenAPI security scheme to the active AUTH_TYPE.

    The `Authorization` scheme is declared once in `api/context.py`, at
    import time — before config is read — so its description can't know
    which provider runs. The app factory does, and rewrites the schema
    here: provider-specific wording for `ckan` / `jwt`; under `anonymous`
    the scheme and every operation's `security` entry are removed, so the
    docs render no Authorize button at all.
    """
    default_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        schema = default_openapi()
        schemes = schema.get("components", {}).get("securitySchemes", {})
        if auth_type == "anonymous":
            schemes.pop("Authorization", None)
            if not schemes:
                schema.get("components", {}).pop("securitySchemes", None)
            for path_item in schema.get("paths", {}).values():
                for operation in path_item.values():
                    if not isinstance(operation, dict):
                        continue
                    security = [
                        requirement
                        for requirement in operation.get("security", [])
                        if "Authorization" not in requirement
                    ]
                    if security:
                        operation["security"] = security
                    else:
                        operation.pop("security", None)
        elif auth_type in _AUTH_SCHEME_DESCRIPTIONS and "Authorization" in schemes:
            description = _AUTH_SCHEME_DESCRIPTIONS[auth_type]
            schemes["Authorization"]["description"] = description
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


def strip_default_422(app: FastAPI) -> None:
    """Drop FastAPI's auto-generated 422 from the schema.

    `RequestValidationError` is remapped to a 400 CKAN error envelope (see
    `error_handlers`), so a documented 422 never actually occurs — the real
    4xx shapes are declared via `ERROR_RESPONSES`.
    """
    default_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        schema = default_openapi()
        for path_item in schema.get("paths", {}).values():
            for operation in path_item.values():
                if isinstance(operation, dict):
                    operation.get("responses", {}).pop("422", None)
        components = schema.get("components", {}).get("schemas", {})
        components.pop("HTTPValidationError", None)
        components.pop("ValidationError", None)
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


API_DESCRIPTION = """
The Datastore API is a RESTful service for managing tabular data.
It provides endpoints for creating, updating, deleting, and querying records,
as well as downloading filtered or complete data in various formats.

%(auth)s
"""


# The auth paragraph of `API_DESCRIPTION`, per `AUTH_TYPE`. Keyed the same way
# as `_AUTH_SCHEME_DESCRIPTIONS` (which words the Authorize modal), so the two
# never disagree about how a caller authenticates. A provider not listed here —
# a third-party drop-in under `datastore/auth/<name>/` — gets the generic
# fallback rather than another provider's instructions.
_AUTH_DESCRIPTIONS = {
    "ckan": (
        "### Authentication \n"
        "Send a CKAN API token as-is in the "
        "`Authorization` header - no `Bearer` prefix. Use **Authorize** above "
        "to try authenticated calls from this page."
    ),
    "jwt": (
        "**Authentication.** Send a signed JWT in the `Authorization` "
        "header, either raw or as `Bearer <token>`."
    ),
    "anonymous": (
        "**Authentication.** No credentials are required. The API is open to all callers."
    ),
}

_AUTH_DESCRIPTION_FALLBACK = (
    "**Authentication.** Send your credentials in the `Authorization` header. "
    "Use **Authorize** above to try authenticated calls from this page."
)


def api_description(auth_type: str) -> str:
    """`API_DESCRIPTION` with the paragraph for the active provider spliced in.

    The description is rendered at the top of the docs page, so telling a
    reader to paste a CKAN token when the service runs on JWT (or needs no
    credentials at all) would be actively misleading.
    """
    # A plain replace, not `str.format` / %-formatting: the description may
    # contain literal braces (e.g. a `{resource_id}` path) that both would
    # try to interpret as fields.
    return API_DESCRIPTION.replace(
        "%(auth)s",
        _AUTH_DESCRIPTIONS.get(auth_type, _AUTH_DESCRIPTION_FALLBACK),
        1,
    )


OPENAPI_TAGS = [
    {
        "name": "Health",
        "description": ("Liveness and readiness probes for " "orchestration."),
    },
    {
        "name": "Datastore",
        "description": (
            "API endpoint - create, upsert, delete, search" "search_sql, and info."
        ),
    },
    {
        "name": "Datastore Download",
        "description": "Bulk download of an entire resource in available formats.",
    },
]
