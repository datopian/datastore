"""Streaming response writers for `datastore_search`.

Every writer emits the same CKAN JSON envelope. Only the shape of the
`records` field changes per `records_format`:

    objects — JSON array of `{col: value, ...}` per row
    lists   — JSON array of `[v1, v2, ...]` per row
    csv     — one JSON string containing all rows as CSV text
    tsv     — one JSON string containing all rows as TSV text

The response is always `Content-Type: application/json`; CSV / TSV
clients parse the envelope and read `result.records` as a multi-line
string.

Every chunk is yielded as `bytes` one at a time so peak memory stays
≈ 1 row regardless of result size:

  - the engine's row iterator stays lazy from BigQuery's `RowIterator`
    all the way to `socket.send()`;
  - each row is serialised in isolation (orjson for JSON values,
    `csv.writer` for CSV / TSV row strings) — no intermediate buffer;
  - the surrounding envelope is emitted as fixed prefix / suffix
    chunks around the row loop.

CSV / TSV rows are embedded inside a JSON string value, so each row's
text is JSON-escaped via `orjson.dumps(s)[1:-1]` before being yielded
between the records field's opening / closing `"` quotes.

`zip_archive_writer` at the bottom is the odd one out: it serves the
download endpoints rather than `datastore_search`, and packs several
already-exported files into one archive. Same discipline — bytes are
yielded a chunk at a time and never accumulated.
"""

from __future__ import annotations

import base64
import csv
import io
import zipfile
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal
from typing import Any

import orjson


def _json_default(obj: Any) -> Any:
    """Serialise types `orjson` refuses out of the box.

    BigQuery `NUMERIC` / `BIGNUMERIC` columns come back as
    `decimal.Decimal`. Emit them as JSON numbers so clients can do
    arithmetic without parsing a string. The cost is that values past
    ~15 significant digits round to the nearest IEEE-754 double —
    full-precision callers should `CAST(... AS STRING)` in
    `datastore_search_sql` instead.

    `bytes` (BigQuery `BYTES` columns) are base64-encoded so the
    response stays UTF-8 and round-trippable.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    raise TypeError(
        f"orjson cannot serialise {type(obj).__name__}; "
        "extend `_json_default` if a new BigQuery type comes through."
    )


def stream_objects(
    *,
    help_url: str,
    resource_id: str,
    schema: dict[str, Any],
    fields: list[dict[str, Any]],
    records: Iterator[tuple],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, str],
    sql: str | None = None,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """`records_format=objects` — `records` is a JSON array of `{col: value}`."""
    columns = [f["id"] for f in fields]
    return _stream_envelope(
        help_url=help_url,
        resource_id=resource_id,
        schema=schema,
        fields=fields,
        records_chunks=_records_object_array(columns, records),
        limit=limit,
        offset=offset,
        total=total,
        include_total=include_total,
        links=links,
        sql=sql,
        warnings=warnings,
    )


def stream_lists(
    *,
    help_url: str,
    resource_id: str,
    schema: dict[str, Any],
    fields: list[dict[str, Any]],
    records: Iterator[tuple],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, str],
    sql: str | None = None,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """`records_format=lists` — `records` is a JSON array of `[v1, v2, ...]`."""
    return _stream_envelope(
        help_url=help_url,
        resource_id=resource_id,
        schema=schema,
        fields=fields,
        records_chunks=_records_array_array(records),
        limit=limit,
        offset=offset,
        total=total,
        include_total=include_total,
        links=links,
        sql=sql,
        warnings=warnings,
    )


def stream_csv(
    *,
    help_url: str,
    resource_id: str,
    schema: dict[str, Any],
    fields: list[dict[str, Any]],
    records: Iterator[tuple],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, str],
    sql: str | None = None,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """`records_format=csv` — `records` is a JSON string of CSV text."""
    columns = [f["id"] for f in fields]
    return _stream_envelope(
        help_url=help_url,
        resource_id=resource_id,
        schema=schema,
        fields=fields,
        records_chunks=_records_delimited_string(columns, records, delimiter=","),
        limit=limit,
        offset=offset,
        total=total,
        include_total=include_total,
        links=links,
        sql=sql,
        warnings=warnings,
    )


def stream_tsv(
    *,
    help_url: str,
    resource_id: str,
    schema: dict[str, Any],
    fields: list[dict[str, Any]],
    records: Iterator[tuple],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, str],
    sql: str | None = None,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """`records_format=tsv` — `records` is a JSON string of TSV text."""
    columns = [f["id"] for f in fields]
    return _stream_envelope(
        help_url=help_url,
        resource_id=resource_id,
        schema=schema,
        fields=fields,
        records_chunks=_records_delimited_string(columns, records, delimiter="\t"),
        limit=limit,
        offset=offset,
        total=total,
        include_total=include_total,
        links=links,
        sql=sql,
        warnings=warnings,
    )


def _stream_envelope(
    *,
    help_url: str,
    resource_id: str,
    schema: dict[str, Any],
    fields: list[dict[str, Any]],
    records_chunks: Iterator[bytes],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, str],
    sql: str | None = None,
    warnings: list[str] | None = None,
) -> Iterator[bytes]:
    """CKAN envelope skeleton. Each format passes its own `records_chunks`
    iterator that emits the JSON value for the `records` field — either
    a JSON array (objects / lists) or a JSON string (csv / tsv).

    Column metadata is emitted in both shapes: `schema` (canonical
    Frictionless) and `fields` (legacy `{id, type}` list, deprecated).
    `sql` is emitted only when supplied (i.e. for `datastore_search_sql`);
    `datastore_search` leaves it out. `warnings` (deprecated-input notices)
    is emitted at the envelope level — a sibling of `result`, matching
    `_success_response` — and omitted when empty.
    """
    yield b'{"help":'
    yield orjson.dumps(help_url)
    yield b',"success":true,"result":{"resource_id":'
    yield orjson.dumps(resource_id)
    if sql is not None:
        yield b',"sql":'
        yield orjson.dumps(sql)
    yield b',"schema":'
    yield orjson.dumps(schema)
    yield b',"fields":'
    yield orjson.dumps(fields)
    yield b',"records":'
    yield from records_chunks
    yield b',"limit":'
    yield orjson.dumps(limit)
    yield b',"offset":'
    yield orjson.dumps(offset)
    if include_total and total is not None:
        yield b',"total":'
        yield orjson.dumps(total)
    yield b',"_links":'
    yield orjson.dumps(links)
    yield b"}"  # close `result`
    if warnings:
        yield b',"warnings":'
        yield orjson.dumps(warnings)
    yield b"}"  # close envelope


def _records_object_array(
    columns: list[str], records: Iterator[tuple]
) -> Iterator[bytes]:
    """`[{col: value, ...}, ...]`."""
    yield b"["
    first = True
    for row in records:
        if first:
            first = False
        else:
            yield b","
        yield orjson.dumps(dict(zip(columns, row)), default=_json_default)
    yield b"]"


def _records_array_array(records: Iterator[tuple]) -> Iterator[bytes]:
    """`[[v1, v2, ...], ...]`."""
    yield b"["
    first = True
    for row in records:
        if first:
            first = False
        else:
            yield b","
        yield orjson.dumps(list(row), default=_json_default)
    yield b"]"


def _records_delimited_string(
    columns: list[str], records: Iterator[tuple], *, delimiter: str
) -> Iterator[bytes]:
    """`"col1,col2\\nv1,v2\\n..."` — one JSON string containing CSV / TSV text.

    Yields:
      1. `"`            — opening quote of the JSON string value
      2. header row     — `csv.writer`-encoded then JSON-escaped
      3. data rows      — same per row
      4. `"`            — closing quote
    """
    yield b'"'
    for row in records:
        yield _json_string_inner(_delimited_row(row, delimiter=delimiter))
    yield b'"'


def _delimited_row(row: Any, *, delimiter: str) -> str:
    """One CSV / TSV row as a `str` including the trailing newline.

    Uses `csv.writer` for RFC 4180 quoting / escaping. The per-row
    `StringIO` is constant-size so memory stays bounded.
    """
    buf = io.StringIO()
    csv.writer(
        buf, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
    ).writerow(row)
    return buf.getvalue()


def _json_string_inner(s: str) -> bytes:
    """JSON-encode `s` and return the bytes BETWEEN the outer quotes.

    `orjson.dumps("a\\nb")` returns `b'"a\\\\nb"'`; we strip the outer
    quotes so the caller can splice the escaped content between its own
    opening / closing `"` chunks. This lets us emit a single JSON string
    value chunk-by-chunk across many rows without materialising it.
    """
    return orjson.dumps(s)[1:-1]


# ---------------------------------------------------------------------------
# Zip archive over already-exported files (multi-file download)
# ---------------------------------------------------------------------------

# Read size from storage, and roughly how often bytes reach the client.
_ZIP_CHUNK_BYTES = 1 << 20

# Per-operation timeout while pulling a part. The client's default is
# tuned for CKAN's small JSON calls; a multi-GB shard needs far more
# headroom, and a timeout mid-archive truncates the download.
_ZIP_FETCH_TIMEOUT = 300.0


class _ZipSink:
    """The write-only file object `zipfile` builds the archive into.

    `ZipFile` probes for `tell()`; finding none it switches to its
    non-seekable mode, which writes each member's sizes and CRC in a
    trailing data descriptor instead of seeking back to patch the local
    header. That is precisely what makes the archive streamable.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def write(self, data: bytes) -> int:
        self._buf += data
        return len(data)

    def flush(self) -> None:
        return None

    def __len__(self) -> int:
        return len(self._buf)

    def drain(self) -> bytes:
        """Hand over everything buffered so far and reset."""
        out = bytes(self._buf)
        del self._buf[:]
        return out


async def zip_archive_writer(
    http: Any,
    members: list[tuple[str, str]],
) -> AsyncIterator[bytes]:
    """Stream a zip of `members` — `(filename, url)` pairs — as it builds.

    Each URL is fetched a chunk at a time and framed straight into the
    archive, so peak memory is one chunk rather than one file. `http` is
    an `httpx.AsyncClient` supplied by the caller (services never build
    their own).

    Entries are **stored, never deflated**: the payload is parquet,
    already compressed, so deflating would burn 30–100 MB/s of CPU for
    no size win. The archive is pure framing, and `force_zip64` keeps a
    member over 4 GiB from blowing up at close.

    This is the one download path where the server carries the bytes, so
    it inherits the costs: no `Content-Length` (hence no progress bar),
    and no range support, so a dropped connection restarts the transfer.
    """
    sink = _ZipSink()
    with zipfile.ZipFile(
        sink, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as archive:
        for filename, url in members:
            with archive.open(filename, mode="w", force_zip64=True) as entry:
                async with http.stream(
                    "GET", url, timeout=_ZIP_FETCH_TIMEOUT
                ) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes(_ZIP_CHUNK_BYTES):
                        entry.write(chunk)
                        if len(sink) >= _ZIP_CHUNK_BYTES:
                            yield sink.drain()
            # Member closed → its data descriptor is in the buffer.
            yield sink.drain()
    # Archive closed → central directory + EOCD.
    yield sink.drain()
