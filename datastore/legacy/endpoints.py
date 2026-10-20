"""Legacy CKAN-compatible HTTP endpoints.

DEPRECATED — see `datastore/legacy/__init__.py` for the removal plan.

The one FastAPI-aware module in the legacy package: everything that
touches `Request`, `Response`, `StreamingResponse`, routing, or
`Depends` lives here. The streaming writers and the request schema
subclass are in sibling modules (`streaming.py`, `schemas.py`) so
future maintainers don't accidentally couple business logic to the
legacy wrapper.

Mounted at `/api/3/action/` and `/datastore/dump/` by
`datastore/api/routes.py`. Hidden from the v2 OpenAPI document via
`include_in_schema=False` so Swagger users see only v2 — the legacy
URLs keep working for scripts in the wild, but new integrators are
steered to the current contract.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from fastapi import APIRouter, Body, Query
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from datastore.api.context import Context
from datastore.api.error_handlers import _format_loc, _group_errors
from datastore.api.responses import ERROR_RESPONSES, ORJSONResponse, _success_response
from datastore.core.exceptions import APIError, ValidationError
from datastore.core.helper import content_disposition, dump_filename_base
from datastore.infrastructure.engines import get_datastore_engine
from datastore.infrastructure.engines.registry import get_allowed_sql_functions
from datastore.legacy.constants import (
    LEGACY_DUMP_EXTENSIONS,
    LEGACY_DUMP_MEDIA_TYPES,
    LegacyDumpFormat,
)
from datastore.legacy.schemas import DatastoreLegacySearchSQLRequest
from datastore.legacy.streaming import (
    build_legacy_pagination_links,
    dump_csv,
    dump_json,
    dump_tsv,
    dump_xml,
    legacy_info_result,
    stream_legacy_search,
    stream_legacy_search_sql,
)
from datastore.schemas.request import DatastoreInfoRequest, DatastoreSearchRequest
from datastore.schemas.validators import (
    ResourceId,
    frictionless_schema_to_fields,
    to_csv_list,
    to_json_object,
    to_str_or_json_object,
)

# Signed-int32 max — well under BigQuery's LIMIT ceiling, lets a legacy
# dump without an explicit `?limit=` drain the whole table while still
# passing a concrete int to `engine.search` (which takes `limit: int`,
# not `int | None`). Callers wanting a smaller cap pass `?limit=N`.
_LEGACY_DUMP_UNBOUNDED = 2**31 - 1


_log = logging.getLogger(__name__)


class _CKANCompatRoute(APIRoute):
    """FastAPI route that mimics CKAN action-API error behaviour.

    Real CKAN's action API returns **HTTP 200** with `success:false` for
    every logical error (ValidationError, NotAuthorized, ObjectNotFound,
    …) — the HTTP status code is used only for infrastructure-level
    problems. A legacy client doing

        if response.status_code == 200:
            body = response.json()
            if not body["success"]: ...

    breaks against the v2-style 4xx we emit by default. This route class
    catches the two exception types that are logical errors (our own
    `APIError` with `status<500`, plus FastAPI's `RequestValidationError`
    for malformed queries / bodies) and re-emits them as 200. 5xx is
    left alone — the CKAN docs explicitly allow 500 for "major
    formatting problems", and masking a server fault as 200 would hide
    real breakage from monitoring.

    Applied via `APIRouter(route_class=_CKANCompatRoute)` on the legacy
    action router only. The dump router keeps standard HTTP semantics
    because it serves raw files, not the action envelope.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original = super().get_route_handler()

        async def ckan_handler(request: Request) -> Response:
            try:
                return await original(request)
            except APIError as exc:
                if exc.status_code >= 500:
                    # Server fault — let the global 5xx handler log it
                    # and emit a 5xx so monitoring stays honest. CKAN
                    # itself does the same.
                    raise
                _log.debug(
                    "legacy APIError masked to 200: %s %s -> %s",
                    request.method,
                    request.url.path,
                    exc.type_label,
                )
                return _legacy_error_envelope(
                    request,
                    type_label=exc.type_label,
                    message=exc.message,
                    fields=exc.fields,
                )
            except RequestValidationError as exc:
                errors = list(exc.errors())
                fields = _group_errors(errors)
                first = errors[0] if errors else {"msg": "invalid request"}
                message = f"{_format_loc(tuple(first.get('loc', ())))}: {first.get('msg')}"
                _log.debug(
                    "legacy RequestValidationError masked to 200: %s %s",
                    request.method,
                    request.url.path,
                )
                return _legacy_error_envelope(
                    request,
                    type_label="Validation Error",
                    message=message,
                    fields=fields,
                )

        return ckan_handler


def _legacy_error_envelope(
    request: Request,
    *,
    type_label: str,
    message: str,
    fields: dict[str, list[str]] | None = None,
) -> ORJSONResponse:
    """CKAN-shaped error envelope at HTTP 200.

    Matches the exact shape real CKAN emits, verified against
    api.neso.energy's responses:

        {
          "help": "...",
          "success": false,
          "error": {
            "<field>": ["<msg>", ...],   # per-field keys at ERROR ROOT
            "__type": "..."
          }
        }

    Specifics preserved for 1:1 parity:

      - No top-level `message` key. CKAN encodes the human-readable
        reason inside the per-field arrays (or, when there's no field
        context, under a generic `message` key — but only when fields
        are absent).
      - Per-field details are spread **directly on the error dict**
        (not nested under `fields`). A caller doing
        `error["sql"]` or `error["query"]` must find them there.
      - `__type` is the last key by convention; `orjson` preserves
        insertion order, so we write it last.

    `help` echoes the request URL — same thing the success envelope
    does, so a legacy caller can't tell the two envelopes apart by
    looking at the help key.
    """
    error: dict[str, Any] = {}
    if fields:
        # Per-field details go at the error-dict root (CKAN shape).
        # Field keys never collide with `__type` because we never
        # declare a field literally named `__type`; a defensive guard
        # would be strict-but-wrong (CKAN itself doesn't do it).
        error.update(fields)
    else:
        # No field context — surface the human message under `message`.
        # Dropping it entirely would make the error unparseable for
        # callers that only look at the error dict.
        error["message"] = message
    error["__type"] = type_label

    return ORJSONResponse(
        {"help": str(request.url), "success": False, "error": error},
        status_code=200,
    )


# `include_in_schema=False` keeps the legacy surface out of the v2
# OpenAPI document. Legacy exists purely for backwards compatibility —
# surfacing it in Swagger would advertise URLs we actively want callers
# to migrate off. Scripts targeting the legacy paths keep working; new
# integrators see only v2.
#
# `route_class=_CKANCompatRoute` makes the actions return HTTP 200 on
# logical errors (CKAN semantics). The dump router stays on the default
# route class — it serves raw files, not the JSON envelope, and HTTP
# error codes are the right thing for a browser hitting a bad download
# URL.
router = APIRouter(
    tags=["Datastore (Legacy)"],
    responses=ERROR_RESPONSES,
    include_in_schema=False,
    route_class=_CKANCompatRoute,
)
dump_router = APIRouter(
    tags=["Datastore (Legacy)"],
    responses=ERROR_RESPONSES,
    include_in_schema=False,
)


# --- datastore_search ------------------------------------------------------


@router.get("/datastore_search", summary="Legacy CKAN datastore_search (GET)")
async def datastore_search_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreSearchRequest, Query()],
) -> StreamingResponse:
    return await _legacy_search_impl(request, context, params)


@router.post("/datastore_search", summary="Legacy CKAN datastore_search (POST)")
async def datastore_search_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreSearchRequest, Body()],
) -> StreamingResponse:
    return await _legacy_search_impl(request, context, payload)


async def _legacy_search_impl(
    request: Request,
    context: Context,
    params: DatastoreSearchRequest,
) -> StreamingResponse:
    """Shared body: authorize, run the engine, stream the legacy envelope.

    LIMIT policy mirrors v2 — the engine still enforces
    `Config.SEARCH_RESULT_ROWS_MAX`, so a legacy caller trying to pull a
    million rows at once gets the same 400 as a v2 caller.
    """
    await context.authorize(resource_id=params.resource_id, permission="read")

    max_limit = context.config.SEARCH_RESULT_ROWS_MAX
    if params.limit > max_limit:
        raise ValidationError(
            f"limit greater than {max_limit} is not allowed; "
            "paginate with `offset` to fetch more rows",
            fields={"limit": [f"must be <= {max_limit}"]},
        )

    engine = get_datastore_engine(context, mode="ro")
    result = await asyncio.to_thread(
        engine.search,
        resource_id=params.resource_id,
        filters=to_json_object(params.filters),
        q=to_str_or_json_object(params.q),
        fields=to_csv_list(params.fields),
        distinct=params.distinct,
        plain=params.plain,
        language=params.language,
        limit=params.limit,
        offset=params.offset,
        sort=params.sort,
        include_total=params.include_total,
    )

    fields, _ = frictionless_schema_to_fields(result.schema)
    body_iter = stream_legacy_search(
        help_url=str(request.url),
        resource_id=params.resource_id,
        fields=fields,
        records=result.records,
        limit=params.limit,
        offset=params.offset,
        total=result.total,
        include_total=params.include_total,
        links=build_legacy_pagination_links(
            str(request.url),
            limit=params.limit,
            offset=params.offset,
            total=result.total,
        ),
    )
    return StreamingResponse(body_iter, media_type="application/json")


# --- datastore_search_sql --------------------------------------------------


@router.get("/datastore_search_sql", summary="Legacy CKAN datastore_search_sql (GET)")
async def datastore_search_sql_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreLegacySearchSQLRequest, Query()],
) -> StreamingResponse:
    return await _legacy_search_sql_impl(request, context, params)


@router.post("/datastore_search_sql", summary="Legacy CKAN datastore_search_sql (POST)")
async def datastore_search_sql_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreLegacySearchSQLRequest, Body()],
) -> StreamingResponse:
    return await _legacy_search_sql_impl(request, context, payload)


async def _legacy_search_sql_impl(
    request: Request,
    context: Context,
    params: DatastoreLegacySearchSQLRequest,
) -> StreamingResponse:
    """Authorize every referenced table, enforce the function allow-list,
    run the SQL and stream the legacy SQL envelope.

    LIMIT is optional (legacy CKAN behaviour). **No row cap applies**:
    `datastore_search_sql` is where CKAN historically let callers pull
    arbitrary query results, so a legacy script doing
    `SELECT * FROM "<rid>"` on a 10M-row table streams the whole set.
    `SEARCH_RESULT_ROWS_MAX` still gates `datastore_search`; it does
    not apply here. The engine streams row-by-row so peak memory is
    bounded regardless of result size.
    """
    for resource_id in params.resource_ids:
        await context.authorize(resource_id=resource_id, permission="read")

    allowed = get_allowed_sql_functions(
        context.config.DATASTORE_ENGINE,
        override_path=context.config.SQL_FUNCTIONS_ALLOW_FILE,
    )
    disallowed = sorted(set(params.function_names) - allowed)
    if disallowed:
        raise ValidationError(
            f"sql uses disallowed function(s): {', '.join(disallowed)}",
            fields={"sql": [f"disallowed: {', '.join(disallowed)}"]},
        )

    # No cap: when the caller omits LIMIT we pass int32-max so the
    # engine's `limit: int` signature stays non-optional, matching the
    # dump endpoint's approach. The engine streams; it does not
    # materialise the result set.
    engine = get_datastore_engine(context, mode="ro")
    effective_limit = params.limit if params.limit is not None else _LEGACY_DUMP_UNBOUNDED
    result = await asyncio.to_thread(
        engine.search_sql,
        sql=params.sql,
        limit=effective_limit,
    )
    fields, _ = frictionless_schema_to_fields(result.schema)
    body_iter = stream_legacy_search_sql(
        help_url=str(request.url),
        sql=params.sql,
        fields=fields,
        records=result.records,
    )
    return StreamingResponse(body_iter, media_type="application/json")


# --- datastore_info --------------------------------------------------------


@router.get("/datastore_info", summary="Legacy CKAN datastore_info (GET)")
async def datastore_info_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreInfoRequest, Query()],
):
    return await _legacy_info_impl(request, context, params)


@router.post("/datastore_info", summary="Legacy CKAN datastore_info (POST)")
async def datastore_info_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreInfoRequest, Body()],
):
    return await _legacy_info_impl(request, context, payload)


async def _legacy_info_impl(
    request: Request,
    context: Context,
    params: DatastoreInfoRequest,
):
    """Legacy info: `result.{resource_id, fields, meta}`, fields filtered."""
    # `_require_resource_id_or_id` on `DatastoreInfoRequest` normalises
    # `id` → `resource_id` and rejects the all-None case, so by this point
    # `resource_id` is always set. Narrow the type for mypy.
    assert params.resource_id is not None
    resource_id: str = params.resource_id
    data_dict = await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")
    info = await asyncio.to_thread(engine.info, resource_id=resource_id)

    schema = info.schema
    if context.config.AUTH_TYPE == "ckan":
        ckan_schema = (data_dict.get("resource") or {}).get("schema")
        if ckan_schema:
            schema = ckan_schema
    fields, _ = frictionless_schema_to_fields(schema)

    return _success_response(
        request,
        legacy_info_result(
            resource_id=resource_id,
            fields=fields,
            meta=info.meta,
        ),
    )


# --- dump ------------------------------------------------------------------


_DUMP_WRITERS = {
    "csv": dump_csv,
    "tsv": dump_tsv,
    "json": dump_json,
    "xml": dump_xml,
}


@dump_router.get(
    "/{resource_id}",
    summary="Legacy CKAN datastore dump",
)
async def legacy_dump(
    request: Request,
    context: Context,
    resource_id: ResourceId,
    fmt: Annotated[LegacyDumpFormat, Query(alias="format")] = "csv",
    bom: bool = False,
    # datastore_search filter / projection params — all optional.
    filters: str | None = None,
    q: str | None = None,
    distinct: bool = False,
    plain: bool = True,
    language: str = "english",
    limit: int | None = None,
    offset: int = 0,
    fields: str | None = None,
    sort: str | None = None,
) -> StreamingResponse:
    """Export a resource as a file. Mirrors CKAN's `/datastore/dump/{id}`.

    Formats: `csv` (default) · `tsv` · `json` (JSON array) · `xml`.

    All downloads stream API-side rather than redirecting to GCS — the
    engine's native GCS export doesn't do row-level filtering (hidden
    `_id` / `_updated_at` columns, user `filters` / `q`) and only
    produces csv/ndjson/parquet. One streaming path across formats keeps
    the response behaviour identical for every caller.

    All of `datastore_search`'s filter / projection params are accepted
    (`filters`, `q`, `distinct`, `plain`, `language`, `limit`, `offset`,
    `fields`, `sort`). Omitting `limit` streams the whole table.

    `bom=true` prepends the UTF-8 byte-order mark for Excel-on-Windows
    compatibility.
    """
    await context.authorize(resource_id=resource_id, permission="read")

    # Validate filter payloads up front — same parsers v2 search uses.
    parsed_filters = to_json_object(filters)
    parsed_q = to_str_or_json_object(q)
    parsed_fields = to_csv_list(fields)

    if offset < 0:
        raise ValidationError("offset must be >= 0", fields={"offset": ["must be >= 0"]})
    if limit is not None and limit < 0:
        raise ValidationError("limit must be >= 0", fields={"limit": ["must be >= 0"]})

    effective_limit = limit if limit is not None else _LEGACY_DUMP_UNBOUNDED

    engine = get_datastore_engine(context, mode="ro")
    result = await asyncio.to_thread(
        engine.search,
        resource_id=resource_id,
        filters=parsed_filters,
        q=parsed_q,
        fields=parsed_fields,
        distinct=distinct,
        plain=plain,
        language=language,
        limit=effective_limit,
        offset=offset,
        sort=sort,
        # Row counts are expensive on a full-table dump and the output
        # format carries no envelope to put them in.
        include_total=False,
    )

    legacy_fields, _ = frictionless_schema_to_fields(result.schema)
    writer = _DUMP_WRITERS[fmt]
    body_iter = writer(fields=legacy_fields, records=result.records, bom=bom)

    filename_base = dump_filename_base(None, LEGACY_DUMP_EXTENSIONS[fmt]) or resource_id
    filename = f"{filename_base}.{LEGACY_DUMP_EXTENSIONS[fmt]}"
    return StreamingResponse(
        body_iter,
        media_type=LEGACY_DUMP_MEDIA_TYPES[fmt],
        headers={"Content-Disposition": content_disposition(filename)},
    )
