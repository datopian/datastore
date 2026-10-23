from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from datastore.core.exceptions import ValidationError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.infrastructure.engines.registry import get_allowed_sql_functions
from datastore.schemas.responses import DatastoreInfoResponse
from datastore.schemas.validators import (
    frictionless_schema_to_fields,
    rewrite_sql_offset,
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
    "csv": stream_csv,
    "tsv": stream_tsv,
    "lists": stream_lists,
    "objects": stream_objects,
}


async def search_datastore(
    context: RequestContext,
    data_dict: dict[str, Any],
    *,
    request_url: str,
    warnings: list[str] | None = None,
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
    # Off the event loop — `engine.search` submits the BigQuery query
    # and fetches the first page. The streaming writer below is a sync
    # generator that Starlette runs in its threadpool, so subsequent
    # page fetches also happen off the loop.
    result = await asyncio.to_thread(
        engine.search,
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

    # Prefer the CKAN resource's synced schema when AUTH_TYPE=ckan — same
    # policy as `datastore_info`, so data-dictionary edits made CKAN-side
    # (title / description / unit) are reflected on every read endpoint
    # without a separate `datastore_info` round-trip. Falls back to the
    # engine's stored schema when the resource has none synced.
    schema = result.schema
    if context.config.AUTH_TYPE == "ckan":
        schema = data_dict.get("resource", {}).get("schema") or result.schema
    fields, _ = frictionless_schema_to_fields(schema)

    envelope_kwargs = dict(
        help_url=request_url,
        resource_id=data_dict["resource_id"],
        schema=schema,
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
        warnings=warnings,
    )

    return _WRITERS[data_dict["records_format"]](**envelope_kwargs)


async def search_sql_datastore(
    context: RequestContext,
    data_dict: dict[str, Any],
    *,
    request_url: str,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """Run a vetted SELECT and stream the result.

    `data_dict` carries `sql` + `function_names` + the `limit` /
    `offset` parsed out of the SQL itself (LIMIT is required by the
    request schema; OFFSET defaults to 0). The service:

      - rejects function calls outside the engine's allow-list,
      - clamps LIMIT against `Config.SEARCH_RESULT_ROWS_MAX`,
      - dispatches to the read-only engine (mode="ro" — RO credentials
        are the load-bearing safety),
      - builds CKAN-style pagination links by rewriting the SQL's
        OFFSET so callers can follow `_links.next` / `prev` without
        re-editing their SQL.
    """
    _ensure_allowed_sql_functions(context, data_dict.get("function_names", []))

    limit = data_dict["limit"]
    offset = data_dict["offset"]
    max_limit = context.config.SEARCH_RESULT_ROWS_MAX
    if limit > max_limit:
        raise ValidationError(
            f"LIMIT greater than {max_limit} is not allowed; "
            "paginate with OFFSET to fetch more rows",
            fields={"sql": [f"LIMIT must be <= {max_limit}"]},
        )

    engine = get_datastore_engine(context, mode="ro")
    # Off the event loop — submitting the query + fetching the first
    # page blocks; streaming writer below picks up the rest in threadpool.
    result = await asyncio.to_thread(
        engine.search_sql,
        sql=data_dict["sql"],
        limit=limit,
    )
    # The engine's output schema is the authoritative source — the SELECT
    # may project computed columns or aggregates that don't correspond to
    # any resource's data dictionary. But under AUTH_TYPE=ckan we overlay
    # per-column `info` from any referenced resource whose column name
    # matches an output column, so a bare `SELECT * FROM "<rid>"`
    # inherits the resource's title/description/unit metadata. `type`
    # stays the engine's — a computed column's type is what BigQuery
    # says it is, not what a same-named source column was.
    schema = result.schema
    if context.config.AUTH_TYPE == "ckan":
        schema = overlay_resource_info(
            schema, data_dict.get("resources") or [],
        )
    fields, _ = frictionless_schema_to_fields(schema)
    return stream_objects(
        help_url=request_url,
        resource_id="",
        schema=schema,
        fields=fields,
        records=result.records,
        limit=limit,
        offset=offset,
        total=result.total,
        include_total=result.total is not None,
        links=_build_sql_pagination_links(
            request_url,
            sql=data_dict["sql"],
            limit=limit,
            offset=offset,
            total=result.total,
        ),
        # Echo the original SQL on the response so callers can confirm
        # what actually ran (especially after `_links.next` rewrites
        # the OFFSET on follow-up requests).
        sql=data_dict["sql"],
        warnings=warnings,
    )


async def dump_sql_datastore(context: RequestContext, data_dict: dict[str, Any]) -> list[str]:
    """Export a vetted SELECT's result via the engine; return signed URLs.

    The download-mode sibling of `search_sql_datastore`: same function
    allow-list gate, but **no LIMIT clamp** — the result lands in GCS as
    a file, not in a streamed JSON envelope, so a LIMIT in the SQL is
    honored as written and its absence means the full result set.

    Dispatches on the read-only engine; the EXPORT-side elevation to rw
    credentials is engine-internal (BigQuery writes shards under the rw
    identity).
    """
    _ensure_allowed_sql_functions(context, data_dict.get("function_names", []))
    engine = get_datastore_engine(context, mode="ro")
    # `dump_sql` is a native coroutine (its blocking calls are offloaded
    # internally) — await it directly, no `to_thread`.
    return await engine.dump_sql(
        data_dict["sql"],
        data_dict["fmt"],
        resource_ids=data_dict["resource_ids"],
        function_names=data_dict["function_names"],
        filename_prefix=data_dict.get("filename_prefix"),
    )


# Keys the engine's output schema wins on — the SELECT's result shape
# is BigQuery's authoritative truth about what the query projected.
# `type` + `format` on a computed column (`SELECT AVG(Price) AS p`,
# `SELECT CAST(x AS DATE)`) don't line up with any source column's
# Frictionless type, and overlaying would be a correctness bug.
# Everything else (title, description, comment, unit, example,
# constraints, custom keys, …) is user-managed metadata that the
# resource schema is the authoritative source for.
_ENGINE_WINS_KEYS = frozenset({"type", "format"})


def overlay_resource_info(
    output_schema: dict[str, Any],
    resources: list[dict[str, Any]],
) -> dict[str, Any]:
    """Enrich the engine's output schema with per-column metadata from any
    referenced CKAN resource whose column name matches.

    Policy: the CKAN resource schema wins on every key *except* `type`
    and `format` — those stay the engine's, because a computed SELECT
    column's type is what BigQuery actually returned, not what a
    same-named source column was. This lets a resource's data-dictionary
    edit (title, description, unit, example, custom extras) reach
    `datastore_search_sql` output columns without a second round-trip,
    while an aggregate like `SUM(x) AS x` keeps its real numeric type.

    When two referenced resources carry the same column name, first-match
    wins — resources are listed in FROM order, so the first one named
    takes precedence.

    Called only under `AUTH_TYPE=ckan`; otherwise the engine's schema is
    the only truth we have.
    """
    # name → full source field entry across every referenced resource.
    source_fields: dict[str, dict[str, Any]] = {}
    for resource in resources:
        schema = resource.get("schema") or {}
        for src in schema.get("fields", []):
            name = src.get("name")
            if not name or name in source_fields:
                continue
            source_fields[name] = src

    if not source_fields:
        return output_schema

    enriched_fields: list[dict[str, Any]] = []
    for field in output_schema.get("fields", []):
        name = field.get("name")
        src = source_fields.get(name)
        if src is None:
            enriched_fields.append(field)
            continue
        # Start from the CKAN source (richer metadata), then pin the
        # engine's type/format on top so BigQuery's view of the output
        # column stays authoritative. `name` already matches by lookup.
        merged = dict(src)
        for engine_key in _ENGINE_WINS_KEYS:
            if engine_key in field:
                merged[engine_key] = field[engine_key]
            else:
                merged.pop(engine_key, None)
        enriched_fields.append(merged)

    return {**output_schema, "fields": enriched_fields}


def _ensure_allowed_sql_functions(
    context: RequestContext,
    function_names: list[str],
) -> None:
    """Reject SQL that calls functions outside the engine's allow-list.

    Shared by `search_sql_datastore` and `dump_sql_datastore` so both
    entry points enforce the identical gate.
    """
    allowed = get_allowed_sql_functions(
        context.config.DATASTORE_ENGINE,
        override_path=context.config.SQL_FUNCTIONS_ALLOW_FILE,
    )
    disallowed = sorted(set(function_names) - allowed)
    if disallowed:
        raise ValidationError(
            f"sql uses disallowed function(s): {', '.join(disallowed)}",
            fields={"sql": [f"disallowed: {', '.join(disallowed)}"]},
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
    result = await asyncio.to_thread(
        engine.info,
        resource_id=data_dict["resource_id"],
    )

    schema = result.schema
    meta = result.meta
    if context.config.AUTH_TYPE == "ckan":
        ckan_schema = (data_dict.get("resource") or {}).get("schema")
        if ckan_schema:
            schema = ckan_schema
    fields, primary_key = frictionless_schema_to_fields(schema)

    if "primary_key" in meta and primary_key != meta["primary_key"]:
        meta = {**meta, "primary_key": primary_key}
    return DatastoreInfoResponse.Result(
        meta=meta,
        schema=schema,
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
      - ``page``        — current page = ``offset // limit + 1``, or
        ``null`` when the current page has no rows (empty resource or
        ``offset >= total``). Reporting ``page=5 / total_pages=4``
        would be incoherent, so we emit explicit `null` instead — UI
        can distinguish "no current page" from "field missing".
      - ``total_pages`` — ``ceil(total / limit)``, or ``null`` in the
        same no-rows case. Omitted entirely when ``total`` is unknown
        (``include_total=False``) so we don't fabricate a count.

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
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",
                urlencode(pairs),
                "",
            )
        )

    out: dict[str, Any] = {"start": _qs(base_pairs)}
    if offset > 0:
        prev_offset = max(0, offset - limit)
        out["prev"] = _qs(base_pairs + [("offset", str(prev_offset))])
    has_next = limit > 0 and total is not None and offset + limit < total
    if has_next:
        out["next"] = _qs(base_pairs + [("offset", str(offset + limit))])
    if limit > 0:
        out["page_size"] = limit
    # `total is None` → unknown, assume there might be rows → real ints.
    # Empty resource / past-end → emit explicit `null` so clients can
    # distinguish "no current page exists" from "field forgotten".
    has_rows_on_page = total is None or (total > 0 and offset < total)
    if limit > 0:
        if has_rows_on_page:
            out["page"] = offset // limit + 1
            if total is not None:
                out["total_pages"] = (total + limit - 1) // limit  # ceil div
        elif total is not None:
            out["page"] = None
            out["total_pages"] = None
    return out


def _build_sql_pagination_links(
    url: str,
    *,
    sql: str,
    limit: int,
    offset: int,
    total: int | None,
) -> dict[str, Any]:
    """Pagination links for `datastore_search_sql`.

    Same presence rules as `_build_pagination_links`, but the LIMIT /
    OFFSET live inside the user's SQL — so we can't just bump the
    `offset` query param. Each emitted URL carries a rewritten copy
    of `sql` with a new OFFSET literal (LIMIT is preserved exactly).
    """
    parsed = urlparse(url)
    base_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "sql"]

    def _link_for(target_offset: int) -> str:
        new_sql = rewrite_sql_offset(sql, target_offset)
        pairs = base_pairs + [("sql", new_sql)]
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",
                urlencode(pairs),
                "",
            )
        )

    out: dict[str, Any] = {"start": _link_for(0)}
    if offset > 0:
        out["prev"] = _link_for(max(0, offset - limit))
    has_next = limit > 0 and total is not None and offset + limit < total
    if has_next:
        out["next"] = _link_for(offset + limit)
    if limit > 0:
        out["page_size"] = limit
    has_rows_on_page = total is None or (total > 0 and offset < total)
    if limit > 0:
        if has_rows_on_page:
            out["page"] = offset // limit + 1
            if total is not None:
                out["total_pages"] = (total + limit - 1) // limit
        elif total is not None:
            out["page"] = None
            out["total_pages"] = None
    return out
