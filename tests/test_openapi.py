"""OpenAPI security scheme tracks the active AUTH_TYPE.

The `Authorization` scheme is declared once at import time (see
`api/context.py`), then tailored per app in `create_app()`:

    1. ckan      — description names a CKAN API key only
    2. jwt       — description names a signed JWT only
    3. anonymous — no security scheme at all (no Authorize button)
"""

from __future__ import annotations

from typing import Any

import pytest
from datastore.core.config import get_config
from datastore.main import create_app
from fastapi.testclient import TestClient


def _build_schema(
    monkeypatch: pytest.MonkeyPatch, auth_type: str
) -> dict[str, Any]:
    monkeypatch.setenv("AUTH_TYPE", auth_type)
    get_config.cache_clear()
    return create_app().openapi()


def _scheme(schema: dict[str, Any]) -> dict[str, Any] | None:
    schemes = schema.get("components", {}).get("securitySchemes", {})
    return schemes.get("Authorization")


def _operations(schema: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        operation
        for path_item in schema["paths"].values()
        for operation in path_item.values()
        if isinstance(operation, dict)
    ]


# 1. ckan ---------------------------------------------------------------------

def test_ckan_scheme_describes_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _build_schema(monkeypatch, "ckan")

    scheme = _scheme(schema)
    assert scheme is not None
    assert "CKAN API key" in scheme["description"]
    assert "JWT" not in scheme["description"]


def test_ckan_operations_reference_the_scheme(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Authorize button only works if operations point at the scheme."""
    schema = _build_schema(monkeypatch, "ckan")

    secured = [
        op for op in _operations(schema)
        if any("Authorization" in req for req in op.get("security", []))
    ]
    assert secured, "no operation references the Authorization scheme"


# 2. jwt ----------------------------------------------------------------------

def test_jwt_scheme_describes_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _build_schema(monkeypatch, "jwt")

    scheme = _scheme(schema)
    assert scheme is not None
    assert "JWT" in scheme["description"]
    assert "Bearer <token>" in scheme["description"]
    assert "CKAN" not in scheme["description"]


# 3. anonymous ----------------------------------------------------------------

def test_anonymous_has_no_security_scheme(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _build_schema(monkeypatch, "anonymous")

    assert _scheme(schema) is None


def test_anonymous_operations_carry_no_security(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dangling `security` entry on an operation would render a padlock
    (and an invalid schema) even with the component gone."""
    schema = _build_schema(monkeypatch, "anonymous")

    for operation in _operations(schema):
        assert "security" not in operation


# 4. Swagger UI page ------------------------------------------------------------

def test_docs_page_renders_swagger_ui(client: TestClient) -> None:
    response = client.get("/datastore/api/docs")

    assert response.status_code == 200
    assert "SwaggerUIBundle" in response.text
    assert "/datastore/api/openapi.json" in response.text


def test_docs_page_widens_authorize_input(client: TestClient) -> None:
    """The Authorize modal's token input is ~230px stock — too short to
    see a pasted JWT/API key. The docs page injects CSS to widen it."""
    response = client.get("/datastore/api/docs")

    assert ".auth-container input" in response.text
