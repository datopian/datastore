"""`GET /datastore/dump/{resource_id}` — single download for a table.

Behaviour by shard count (decided by BigQuery from the export size):

  - **1 shard** (≤ 1 GB, including Parquet): 302 redirect to the
    GCS signed URL. Zero server bandwidth — bytes go GCS → client.
  - **N shards** (>1 GB CSV/NDJSON): `StreamingResponse` over
    `services.dump.stream_*_shards`, which pulls each shard from GCS
    via async httpx and byte-forwards (CSV header-dedup; NDJSON pure
    concat). Memory ≈ one chunk in flight; no threadpool consumption.

Multi-shard Parquet is refused with 413 (parquet shards can't be
byte-concatenated). Caller picks CSV/NDJSON.
"""

from __future__ import annotations

import hashlib
from typing import Annotated

from fastapi import APIRouter, Query
from starlette.responses import RedirectResponse, StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import ERROR_RESPONSES
from datastore.core.constants import DumpFormat
from datastore.core.exceptions import ServerError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.request import DatastoreDumpSQLRequest
from datastore.services.dump import (
    stream_csv_shards,
    stream_gzip_csv_shards,
    stream_ndjson_shards,
)
from datastore.services.read import dump_sql_datastore

_MEDIA_TYPE: dict[str, str] = {
    "csv":     "text/csv",
    "gzip":    "application/gzip",
    "ndjson":  "application/x-ndjson",
    "parquet": "application/vnd.apache.parquet",
}

_DOWNLOAD_EXT: dict[str, str] = {
    "csv":     "csv",
    "gzip":    "csv.gz",
    "ndjson":  "ndjson",
    "parquet": "parquet",
}

router = APIRouter(tags=["Datastore Download"], responses=ERROR_RESPONSES)


def shard_download_response(
    urls: list[str], fmt: DumpFormat, download_name: str,
) -> RedirectResponse | StreamingResponse:
    """Shape an export's signed URLs into the download response.

    Shared by `/datastore/dump/{rid}` and `datastore_search_sql`'s
    download mode (both api-layer callers):

      - 1 URL  → 302 redirect; bytes flow GCS → client directly.
      - N URLs → stream-concat the shards into one body (CSV header
        dedup / gzip recompress / NDJSON byte concat).
      - 0 URLs → the engine ran in placeholder mode (no client built);
        an empty file claiming to be a download would be a lie, so 500.
    """
    if not urls:
        raise ServerError(
            "export produced no downloadable files "
            "(datastore engine is not configured)"
        )
    if len(urls) == 1:
        return RedirectResponse(url=urls[0], status_code=302)

    if fmt == "csv":
        body = stream_csv_shards(urls)
    elif fmt == "gzip":
        body = stream_gzip_csv_shards(urls)
    elif fmt == "ndjson":
        body = stream_ndjson_shards(urls)
    else:  # pragma: no cover — the engine rejects multi-shard Parquet
        raise RuntimeError(f"unexpected multi-shard format: {fmt}")

    return StreamingResponse(
        body,
        media_type=_MEDIA_TYPE[fmt],
        headers={
            "Content-Disposition": (
                f'attachment; filename="{download_name}.{_DOWNLOAD_EXT[fmt]}"'
            ),
        },
    )


@router.get(
    "/datastore/dump/sql",
    summary="Download the result of a SQL SELECT (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Single-shard export — redirect to a signed GCS URL."},
        200: {"description": "Multi-shard export — streamed CSV / NDJSON body."},
        413: {
            "description": (
                "Multi-shard Parquet — single-file download isn't possible; "
                "use `format=csv` or `format=ndjson`."
            ),
        },
    },
)
async def dump_sql(
    context: Context,
    params: Annotated[DatastoreDumpSQLRequest, Query()],
):
    """Download the result of a vetted SQL SELECT as one file.

    Same validation as `datastore_search_sql` (single SELECT/WITH,
    per-table auth, function allow-list) but the response is the file
    itself — no CKAN envelope. `LIMIT` is optional: absent exports the
    full result set; present it is honored as written (no row cap).

    Declared **before** `/datastore/dump/{resource_id}`, which makes
    `sql` a reserved resource name on this route family.
    """
    for resource_id in params.resource_ids:
        await context.authorize(resource_id=resource_id, permission="read")

    urls = await dump_sql_datastore(
        context,
        {
            "sql": params.sql,
            "fmt": params.format,
            "resource_ids": params.resource_ids,
            "function_names": params.function_names,
        },
    )
    # Cosmetic identity only (the multi-shard filename); the engine
    # names its cache / 302 filenames from the qualified-SQL hash.
    name = f"query_{hashlib.sha256(params.sql.encode()).hexdigest()[:8]}"
    return shard_download_response(urls, params.format, name)


@router.get(
    "/datastore/dump/{resource_id}",
    summary="Download an entire table (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Single-shard export — redirect to a signed GCS URL."},
        200: {"description": "Multi-shard export — streamed CSV / NDJSON body."},
    },
)
async def dump(
    context: Context,
    resource_id: str,
    fmt: Annotated[DumpFormat, Query(alias="format")] = "csv",
):
    """Download an entire resource as `csv` (default), `gzip`, `ndjson`, or `parquet`.

    Small exports redirect (302) straight to a signed GCS URL; large ones
    stream a concatenated body. Select the format with `?format=`.
    """
    await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")

    urls = await engine.dump(resource_id, fmt)
    return shard_download_response(urls, fmt, resource_id)
