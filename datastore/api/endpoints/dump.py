"""`GET /datastore/dump/{resource_id}` — single download for a table.

Behaviour by shard count (decided by BigQuery from the export size):

  - **1 shard** (≤ 1 GB, or any-size Parquet): 302 redirect to the
    GCS signed URL. Zero server bandwidth — bytes go GCS → client.
  - **N shards** (>1 GB CSV/NDJSON): `StreamingResponse` over
    `services.dump.stream_*_shards`, which pulls each shard from GCS
    via async httpx and byte-forwards (CSV header-dedup; NDJSON pure
    concat). Memory ≈ one chunk in flight; no threadpool consumption.

Parquet >1 GB is refused upstream with 413 (parquet shards can't be
byte-concatenated). Caller picks CSV/NDJSON.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Query
from starlette.responses import RedirectResponse, StreamingResponse

from datastore.api.context import Context
from datastore.infrastructure.engines import get_datastore_engine
from datastore.services.dump import stream_csv_shards, stream_ndjson_shards

DumpFormat = Literal["csv", "ndjson", "parquet"]

_MEDIA_TYPE: dict[str, str] = {
    "csv":     "text/csv",
    "ndjson":  "application/x-ndjson",
    "parquet": "application/vnd.apache.parquet",
}

router = APIRouter(tags=["dump"])


@router.get("/datastore/dump/{resource_id}")
async def dump(
    context: Context,
    resource_id: str,
    fmt: Annotated[DumpFormat, Query(alias="format")] = "csv",
):
    await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")

    urls = await engine.dump(resource_id, fmt)

    if len(urls) == 1:
        return RedirectResponse(url=urls[0], status_code=302)

    if fmt == "csv":
        body = stream_csv_shards(urls)
    elif fmt == "ndjson":
        body = stream_ndjson_shards(urls)
    else:  # pragma: no cover — Parquet never returns >1 shard
        raise RuntimeError(f"unexpected multi-shard format: {fmt}")

    return StreamingResponse(
        body,
        media_type=_MEDIA_TYPE[fmt],
        headers={
            "Content-Disposition": (
                f'attachment; filename="{resource_id}.{fmt}"'
            ),
        },
    )
