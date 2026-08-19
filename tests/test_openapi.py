"""OpenAPI security scheme tracks the active AUTH_TYPE.

The `Authorization` scheme is declared once at import time (see
`api/context.py`), then tailored per app in `create_app()`:

    1. ckan      — description names a CKAN API key only
    2. jwt       — description names a signed JWT only
    3. anonymous — no security scheme at all (no Authorize button)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from datastore.api import docs as docs_module
from datastore.api.docs import api_description
from datastore.core.config import Config, get_config
from datastore.core.constants import API_PREFIX, API_VERSION
from datastore.main import create_app
from fastapi.testclient import TestClient
from pydantic import ValidationError


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
    """Under CKAN auth the scheme describes an API token, and says nothing
    about JWTs — the two providers' wording must not leak into each other."""
    schema = _build_schema(monkeypatch, "ckan")

    scheme = _scheme(schema)
    assert scheme is not None
    assert "API token" in scheme["description"]
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


# 3b. description tracks AUTH_TYPE ------------------------------------------

@pytest.mark.parametrize(
    ("auth_type", "expected", "forbidden"),
    [
        ("ckan", "CKAN API token", "JWT"),
        ("jwt", "signed JWT", "CKAN API token"),
        ("anonymous", "no credentials are required", "CKAN API token"),
        # matched case-insensitively below, so rewording the sentence's
        # opening doesn't break the assertion
    ],
)
def test_description_describes_the_active_provider(
    monkeypatch: pytest.MonkeyPatch,
    auth_type: str,
    expected: str,
    forbidden: str,
) -> None:
    """The description is the first thing a reader sees, so telling them to
    paste a CKAN token when the service runs on JWT would mislead."""
    schema = _build_schema(monkeypatch, auth_type)

    description = schema["info"]["description"]
    assert expected.lower() in description.lower()
    assert forbidden.lower() not in description.lower()


def test_description_falls_back_for_unknown_provider() -> None:
    """A third-party provider under `datastore/auth/<name>/` must not be
    handed another provider's instructions."""
    description = api_description("some-third-party-provider")

    assert "Send your credentials" in description
    assert "CKAN API token" not in description
    assert "signed JWT" not in description


def test_description_placeholder_is_always_substituted() -> None:
    """The auth slot is spliced with a plain replace (the text contains
    literal braces, so `str.format` would raise) — make sure no raw
    placeholder can reach the page."""
    for auth_type in ("ckan", "jwt", "anonymous", "unknown"):
        assert "%(auth)s" not in api_description(auth_type)


def test_description_survives_literal_braces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A path like `/dump/{resource_id}` in the description must survive.

    `str.format`/%-formatting would read those braces as a field and raise at
    startup, so the auth slot is spliced with a plain replace. Asserted against
    a description that actually contains braces, so the guarantee holds even if
    the shipped prose currently has none.
    """
    monkeypatch.setattr(
        docs_module,
        "API_DESCRIPTION",
        "See `/dump/{resource_id}`.\n\n%(auth)s",
    )

    rendered = api_description("ckan")

    assert "{resource_id}" in rendered
    assert "CKAN API token" in rendered


# 3c. contract version, not build version -----------------------------------

def test_info_version_is_the_api_contract_version(client: TestClient) -> None:
    """`info.version` describes the API contract, not the installed build.

    The two are independent: the package can ship any number of releases
    without the contract changing. A build number here would tell a client
    nothing about which request/response shapes it is looking at.
    """
    schema = client.get(f"{API_PREFIX}/openapi.json").json()

    assert schema["info"]["version"] == API_VERSION


def test_info_version_matches_the_url_prefix(client: TestClient) -> None:
    """The documented contract version and the one in the URL are the same
    value, so a schema can never advertise a version its routes don't serve."""
    schema = client.get(f"{API_PREFIX}/openapi.json").json()

    documented = schema["info"]["version"]
    assert any(
        path.startswith(f"/datastore/api/{documented}/")
        for path in schema["paths"]
    ), f"no route served under the documented version {documented!r}"


# 4. Swagger UI page ------------------------------------------------------------

def test_docs_page_renders_swagger_ui(client: TestClient) -> None:
    response = client.get("/datastore/api/v2/docs")

    assert response.status_code == 200
    assert "SwaggerUIBundle" in response.text
    assert "/datastore/api/v2/openapi.json" in response.text


def test_docs_page_serves_vendored_assets(client: TestClient) -> None:
    """Swagger UI is vendored, not pulled from a CDN, so `/docs` renders
    in air-gapped deployments."""
    response = client.get("/datastore/api/v2/docs")

    assert "cdn.jsdelivr.net" not in response.text
    assert "/datastore/api/v2/static/swagger-ui/swagger-ui-bundle.js" in response.text
    assert "/datastore/api/v2/static/theme/theme.css" in response.text

    for asset in (
        "/datastore/api/v2/static/swagger-ui/swagger-ui.css",
        "/datastore/api/v2/static/swagger-ui/swagger-ui-bundle.js",
        "/datastore/api/v2/static/theme/theme.css",
    ):
        assert client.get(asset).status_code == 200, asset


def test_docs_page_widens_authorize_input() -> None:
    """The Authorize modal's token input is ~230px stock — too short to see
    a pasted JWT/API key. The theme widens the modal and the input."""
    css = (
        Path(__file__).resolve().parent.parent
        / "datastore/api/static/theme/theme.css"
    ).read_text()

    assert "max-width: 900px" in css
    assert ".swagger-ui .auth-container input" in css


def test_docs_page_applies_theme_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """`DOCS_*` env vars reach the page's CSS custom properties and header,
    so a deployment rebrands without shipping CSS."""
    monkeypatch.setenv("DOCS_PRIMARY_COLOR", "#7A3864")
    monkeypatch.setenv("DOCS_HEADER_COLOR", "#123456")
    monkeypatch.setenv("DOCS_SITE_TITLE", "NESO Datastore API")
    monkeypatch.setenv("DOCS_LOGO_URL", "/static/logo.png")
    get_config.cache_clear()

    try:
        with TestClient(create_app()) as themed_client:
            body = themed_client.get("/datastore/api/v2/docs").text
    finally:
        get_config.cache_clear()

    assert "--docs-primary: #7A3864;" in body
    assert "--docs-header-bg: #123456;" in body
    assert "NESO Datastore API" in body
    assert '<img class="docs-logo" src="/static/logo.png"' in body


def test_docs_page_brands_header_from_primary_color(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`DOCS_PRIMARY_COLOR` alone must brand the whole page.

    With `DOCS_HEADER_COLOR` unset no `--docs-header-bg` is emitted, so the
    stylesheet's `--docs-header-bg: var(--docs-primary)` fallback applies and
    the header takes the brand colour. A non-empty default here would pin the
    bar to a fixed grey and make branding look broken.
    """
    monkeypatch.setenv("DOCS_PRIMARY_COLOR", "#7A3864")
    get_config.cache_clear()

    try:
        assert Config().DOCS_HEADER_COLOR == ""
        with TestClient(create_app()) as themed_client:
            body = themed_client.get("/datastore/api/v2/docs").text
    finally:
        get_config.cache_clear()

    assert "--docs-primary: #7A3864;" in body
    assert "--docs-header-bg" not in body


def test_docs_page_rejects_non_css_color(monkeypatch: pytest.MonkeyPatch) -> None:
    """The colour lands inside a `<style>` block, so it is validated at
    config load rather than injected as given."""
    monkeypatch.setenv("DOCS_PRIMARY_COLOR", "red; } body { display: none")
    get_config.cache_clear()

    try:
        with pytest.raises(ValidationError, match="is not a CSS colour"):
            Config()
    finally:
        get_config.cache_clear()


def test_docs_page_escapes_header_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """The template is rendered with autoescaping on, so header text can't
    break out of its element. `DOCS_SITE_TITLE` / `DOCS_LOGO_URL` are free
    text (unlike the colours, which `Config` validates)."""
    monkeypatch.setenv("DOCS_SITE_TITLE", "<script>alert(1)</script>")
    monkeypatch.setenv("DOCS_LOGO_URL", '"><script>alert(2)</script>')
    get_config.cache_clear()

    try:
        with TestClient(create_app()) as themed_client:
            body = themed_client.get("/datastore/api/v2/docs").text
    finally:
        get_config.cache_clear()

    assert "<script>alert(1)</script>" not in body
    assert "<script>alert(2)</script>" not in body
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in body


def test_docs_page_falls_back_to_openapi_title(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no `DOCS_SITE_TITLE`, the header is never left unlabelled.

    Pinned explicitly rather than relying on the ambient environment: `Config`
    reads `.env`, so a developer who sets a title locally would otherwise see
    this fail.
    """
    monkeypatch.setenv("DOCS_SITE_TITLE", "")
    get_config.cache_clear()

    try:
        with TestClient(create_app()) as bare_client:
            body = bare_client.get("/datastore/api/v2/docs").text
    finally:
        get_config.cache_clear()

    assert '<h1 class="docs-header-title">Datastore API</h1>' in body
