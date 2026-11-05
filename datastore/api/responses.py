from __future__ import annotations

import base64
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import orjson
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastore.api.docs import help_url
from datastore.core.constants import API_PREFIX
from datastore.schemas.responses import ErrorEnvelope


# Shared OpenAPI doc for the CKAN error envelope, attached at router level
# so every action documents the real 4xx / 5xx shape instead of FastAPI's
# default 422. The taxonomy matches `core.exceptions.APIError`.
#
# Each status carries its own example — otherwise Swagger reuses the one
# example declared on `ErrorEnvelope` for every row and callers see a
# `Validation Error` body documented against 404 / 409 / 500.
def _error_example(type_label: str, message: str) -> dict[str, Any]:
    return {
        "application/json": {
            "example": {
                # Relative: the public host comes from `API_URL`, which only
                # `api/docs.py` can read (`schemas/` must not import config).
                "help": f"{API_PREFIX}/docs",
                "success": False,
                "error": {"__type": type_label, "message": message},
            }
        }
    }


ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {
        "model": ErrorEnvelope,
        "description": "Validation Error",
        "content": _error_example("Validation Error", "resource 'foo' is not declared"),
    },
    403: {
        "model": ErrorEnvelope,
        "description": "Authorization Error",
        "content": _error_example(
            "Authorization Error", "user not authorized to read resource 'foo'"
        ),
    },
    404: {
        "model": ErrorEnvelope,
        "description": "Not Found Error",
        "content": _error_example("Not Found Error", "resource 'foo' not found"),
    },
    409: {
        "model": ErrorEnvelope,
        "description": "Conflict Error",
        "content": _error_example("Conflict Error", "resource 'foo' already exists"),
    },
    500: {
        "model": ErrorEnvelope,
        "description": "Internal Error",
        "content": _error_example("Internal Error", "an internal error occurred"),
    },
}


def _orjson_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    # BigQuery `NUMERIC` / `BIGNUMERIC` columns come back as Decimal —
    # JSON has no native form, and orjson refuses by default. Emit as a
    # JSON number so clients can do arithmetic without parsing a string;
    # values past ~15 significant digits round to the nearest IEEE-754
    # double (full-precision callers should CAST to STRING in SQL).
    if isinstance(obj, Decimal):
        f = float(obj)
        if f.is_integer() and abs(f) < 2**63:
            return int(f)
        return f
    # `BYTES` columns come back as raw `bytes`; base64-encode so the
    # response stays UTF-8 and round-trippable.
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, default=_orjson_default)


def _help(request: Request) -> str:
    """Swagger deep link for the endpoint that served this request.

    Resolved from the matched route, so an unrouted request (a 404) falls
    back to the request URL rather than linking to nothing.
    """
    route = request.scope.get("route")
    return help_url(request, getattr(route, "name", None))


def _deprecation_warnings(payload: BaseModel, provided: Iterable[str] | None = None) -> list[str]:
    """Build body-level warnings from `Field(deprecated=...)` metadata.

    For every field the caller explicitly provided whose declaration
    carries a `deprecated` string, emit one warning of the form
    ``"'<field>' is deprecated — <message>."``. Pulling the message off
    the model keeps the wording in one place — the field's own
    declaration — so endpoints never duplicate it.

    "Did the caller send this?" is answered differently per request style:

    - JSON body (`payload=...`): `model_fields_set` is reliable — Pydantic
      records only the keys actually present. Pass `provided=None`.
    - Query params (`Annotated[Model, Query()]`): FastAPI fills
      `model_fields_set` with *every* field (defaults included), so it
      can't tell sent-vs-default apart. Pass the request's query keys as
      `provided` (e.g. `request.query_params.keys()`); a deprecated field
      then warns only when its name appears in the query string.

    Either way the field value is never read — that would itself emit a
    `DeprecationWarning` we don't want at runtime.
    """
    fields = type(payload).model_fields
    sent = set(provided) if provided is not None else payload.model_fields_set
    out: list[str] = []
    for name, field in fields.items():
        if name not in sent:
            continue
        msg = field.deprecated
        if isinstance(msg, str) and msg:
            out.append(f"'{name}' is deprecated — {msg}.")
    return out


def _success_response(
    request: Request,
    result: BaseModel | dict[str, Any],
    *,
    status_code: int = 200,
    warnings: list[str] | None = None,
) -> ORJSONResponse:
    # `result` may be a Pydantic model or a plain dict; orjson's default
    # handler in `_orjson_default` dumps Pydantic models via `model_dump()`.
    # `warnings` is non-fatal advisory text (e.g. deprecated-input notices) —
    # surfaced at envelope level so any client reading the body sees them
    # without having to parse the result block. Omitted when empty.
    body: dict[str, Any] = {
        "help": _help(request),
        "success": True,
        "result": result,
    }
    if warnings:
        body["warnings"] = warnings
    return ORJSONResponse(body, status_code=status_code)


def _error_response(
    request: Request,
    *,
    status_code: int,
    type_label: str,
    message: str,
    fields: dict[str, list[str]] | None = None,
) -> ORJSONResponse:
    error: dict[str, Any] = {"__type": type_label, "message": message}
    if fields:
        error["fields"] = fields
    return ORJSONResponse(
        {"help": _help(request), "success": False, "error": error},
        status_code=status_code,
    )
