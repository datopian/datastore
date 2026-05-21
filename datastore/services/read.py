from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from datastore.core.exceptions import ValidationError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.infrastructure.engines.registry import get_allowed_sql_functions
from datastore.schemas.responses import DatastoreInfoResponse
from datastore.schemas.validators import (
    frictionless_schema_to_fields,
    to_csv_list,
    to_json_object,
    to_str_or_json_object,
)
from datastore.services.streaming import (
    stream_csv,
    stream_lists,
    stream_objects,
    stream_tsv,
)

if TYPE_CHECKING:
    from datastore.api.context import RequestContext


_WRITERS = {
    "csv":     stream_csv,
    "tsv":     stream_tsv,
    "lists":   stream_lists,
    "objects": stream_objects,
}


async def search_datastore(
    context: RequestContext,
    data_dict: dict[str, Any],
    *,
    request_url: str,
) -> Iterator[bytes]:
    """Run the search and return a lazy `bytes` iterator over the JSON
    response body.

    All four `records_format` writers emit the same CKAN envelope and
    Content-Type (`application/json`); only the shape of `records` inside
    differs, so the endpoint hardcodes the media type and just wraps the
    returned iterator in a `StreamingResponse`.

    `data_dict` is the auth result merged with `params.model_dump()`, so
    every schema field is present with its default. `request_url` is
    passed in as a string because services can't import Starlette — we
    parse it with `urllib.parse` to build the relative `_links`.

    The returned iterator pulls rows from the engine one at a time;
    peak memory ≈ 1 row regardless of result size.
    """
    max_limit = context.config.SEARCH_RESULT_ROWS_MAX
    if data_dict["limit"] > max_limit:
        raise ValidationError(
            f"limit greater than {max_limit} is not allowed; "
            "paginate with `offset` to fetch more rows",
            fields={"limit": [f"must be <= {max_limit}"]},
        )

    engine = get_datastore_engine(context, mode="ro")
    result = engine.search(
        resource_id=data_dict["resource_id"],
        filters=to_json_object(data_dict["filters"]),
        q=to_str_or_json_object(data_dict["q"]),
        fields=to_csv_list(data_dict["fields"]),
        distinct=data_dict["distinct"],
        plain=data_dict["plain"],
        language=data_dict["language"],
        limit=data_dict["limit"],
        offset=data_dict["offset"],
        sort=data_dict["sort"],
        include_total=data_dict["include_total"],
    )

    fields, _ = frictionless_schema_to_fields(result.schema)
    
    envelope_kwargs = dict(
        help_url=request_url,
        resource_id=data_dict["resource_id"],
        schema=result.schema,
        fields=fields,
        records=result.records,
        limit=data_dict["limit"],
        offset=data_dict["offset"],
        total=result.total,
        include_total=data_dict["include_total"],
        links=_build_pagination_links(
            request_url,
            limit=data_dict["limit"],
            offset=data_dict["offset"],
            total=result.total,
        ),
    )

    return _WRITERS[data_dict["records_format"]](**envelope_kwargs)


_SQL_DEFAULT_LIMIT = 32000


async def search_sql_datastore(
    context: RequestContext,
    data_dict: dict[str, Any],
    *,
    request_url: str,
) -> Iterator[bytes]:
    """Run a raw SQL SELECT and stream the result.

    Reuses the `datastore_search` writer + envelope so the response shape
    is identical to `datastore_search`. Pagination is the caller's job
    (edit the SQL); the envelope's `_links` / `limit` / `offset` /
    `resource_id` fields are kept for shape parity, with no-op defaults.

    `data_dict` carries `{"sql": ..., "function_names": [...]}`. The
    endpoint already handles per-table CKAN authorize (using the schema's
    `resource_ids`); this layer handles the engine-specific function
    allow-list — `mode="ro"` selects read-only credentials so writes
    can't happen even if a function slips through.
    """
    allowed = get_allowed_sql_functions(
        context.config.DATASTORE_ENGINE,
        override_path=context.config.SQL_FUNCTIONS_ALLOW_FILE,
    )
    disallowed = sorted(set(data_dict.get("function_names", [])) - allowed)
    
    if disallowed:
        raise ValidationError(
            f"sql uses disallowed function(s): {', '.join(disallowed)}",
            fields={"sql": [f"disallowed: {', '.join(disallowed)}"]},
        )

    engine = get_datastore_engine(context, mode="ro")
    result = engine.search_sql(
        sql=data_dict["sql"], limit=_SQL_DEFAULT_LIMIT
    )
    fields, _ = frictionless_schema_to_fields(result.schema)
    return stream_objects(
        help_url=request_url,
        resource_id="",
        schema=result.schema,
        fields=fields,
        records=result.records,
        limit=_SQL_DEFAULT_LIMIT,
        offset=0,
        total=None,
        include_total=False,
        links=_build_pagination_links(
            request_url, limit=_SQL_DEFAULT_LIMIT, offset=0, total=None,
        ),
    )


async def info_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreInfoResponse.Result:
    """Look up table metadata for a single `resource_id`.

    Endpoint authorizes the caller first (same gate as `search`). This
    service just asks the read-only engine for its `InfoResult` and
    re-shapes it as the response's typed `Result`. No streaming —
    `info` responses are small enough for the standard `_success_response`
    path.
    """
    engine = get_datastore_engine(context, mode="ro")
    result = engine.info(resource_id=data_dict["resource_id"])
    fields, _ = frictionless_schema_to_fields(result.schema)
    return DatastoreInfoResponse.Result(
        meta=result.meta,
        schema=result.schema,
        fields=fields,
    )


def _build_pagination_links(
    url: str,
    *,
    limit: int,
    offset: int,
    total: int | None = None,
) -> dict[str, Any]:
    """CKAN-style pagination links + page counters.

    URL keys:
      - ``start`` — always emitted, with ``offset`` stripped (defaults to 0).
      - ``prev``  — only when a previous page exists (``offset > 0``);
        lands at ``max(0, offset - limit)`` so paging back from a partial
        first page clamps to 0 rather than going negative.
      - ``next``  — only when a next page exists (``total`` known and
        ``offset + limit < total``). When ``total`` is None (caller
        didn't ask for `include_total`, or this is a raw-SQL call) we
        can't tell, so ``next`` is omitted; the client detects end-of-
        data via an empty `records` array.

    Counter keys (added alongside the URL keys, 1-indexed):
      - ``page_size``   — rows per page = ``limit``; emitted whenever
        ``limit > 0`` (a UI can always render it, even on empty pages).
      - ``page``        — current page = ``offset // limit + 1``.
      - ``total_pages`` — ``ceil(total / limit)``; omitted when total
        is unknown.

    ``page`` and ``total_pages`` are dropped whenever the current page
    has no rows — either because the resource is empty
    (``total == 0``) or because the caller paged past the end
    (``offset >= total > 0``). Reporting ``page=5 / total_pages=4``
    would be incoherent (no such page exists); the absence + the
    empty `records` array + `prev` are what let a UI recover. When
    ``total`` is unknown (caller didn't request `include_total`) we
    keep ``page`` since position is still meaningful for single-page
    pickers.

    All non-`offset` query params ride along on every emitted URL.

    Scheme + host are preserved from the input URL when present, so
    `http://host/path?x=1` produces `http://host/path?...` links and
    a bare `/path?x=1` produces bare-path links. `urllib.parse` is
    used instead of Starlette's `URL` helpers because services don't
    import Starlette (CLAUDE.md §3 layer rule).
    """
    parsed = urlparse(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    base_pairs = [(k, v) for k, v in pairs if k != "offset"]

    def _qs(pairs: list[tuple[str, str]]) -> str:
        return urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            "", urlencode(pairs), "",
        ))

    out: dict[str, Any] = {"start": _qs(base_pairs)}
    if offset > 0:
        prev_offset = max(0, offset - limit)
        out["prev"] = _qs(base_pairs + [("offset", str(prev_offset))])
    has_next = (
        limit > 0 and total is not None and offset + limit < total
    )
    if has_next:
        out["next"] = _qs(base_pairs + [("offset", str(offset + limit))])
    if limit > 0:
        out["page_size"] = limit
    # Drop `page` / `total_pages` whenever the current page has no
    # rows: empty resource or past-end pagination. `total is None`
    # means "unknown → assume there might be rows" so we still emit
    # `page`.
    has_rows_on_page = total is None or (total > 0 and offset < total)
    if limit > 0 and has_rows_on_page:
        out["page"] = offset // limit + 1
        if total is not None:
            # ceil division without importing math
            out["total_pages"] = (total + limit - 1) // limit
    return out
