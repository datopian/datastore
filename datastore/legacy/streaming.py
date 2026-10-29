"""Streaming response writers for the legacy CKAN datastore API.

DEPRECATED — see `datastore/legacy/__init__.py` for the removal plan.

A sibling of `datastore/services/streaming.py` (the v2 writers), but
kept in the legacy package so the whole surface deletes together.
Same chunked-bytes discipline (peak memory ≈ 1 row) and the same
reuse of `_json_default` from `services/streaming.py` for Decimal /
bytes handling. The envelope differs — the one real CKAN emits — and
engine-managed system columns (`_id`, `_updated_at`) are stripped so
legacy callers see exactly the shape they see today against the
public CKAN API.

Deltas vs `datastore/services/streaming.py`:

  - No `schema` block. Legacy CKAN only knows the `fields` list.
  - All four `records_format` values are honored (`objects` /
    `lists` / `csv` / `tsv`), matching the public CKAN API.
  - No `warnings` envelope — legacy clients never read it.
  - `fields` entries carry `{"type", "id", "info"?}` — `info` is
    passed through when the resource schema set one, so `datastore_search`
    and `datastore_search_sql` surface the same per-column metadata
    (title, description, unit, example, …) that `datastore_info`
    returns. The system columns are filtered out by id.
  - Each row tuple is projected to the kept-column indices before
    serialisation so `_id` / `_updated_at` never land in `records`.

The second half of this module (`dump_*`) are file-download writers
rather than envelope writers — they emit raw CSV / TSV / JSON array /
XML bytes rather than the CKAN JSON envelope. CKAN 2.9 serves these
at `/datastore/dump/{id}?format=…`; see the maintainer docs §9.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse

import orjson
import snowballstemmer

from datastore.legacy.constants import (
    LEGACY_DUMP_HIDDEN_COLUMNS,
    LEGACY_FULL_TEXT_FIELD,
    LEGACY_HIDDEN_COLUMNS,
    UTF8_BOM,
)
from datastore.services.streaming import (
    _delimited_row,
    _json_default,
    _json_string_inner,
)

# Postgres's `english_stem` dictionary (`tsearch_data/english.stop` in the
# PG source). Used verbatim so our tsvector output matches what a Postgres-
# backed CKAN datastore emits byte-for-byte. Words in this set are dropped
# BUT the token's position in the sequence is still consumed — so later
# tokens keep their original indices, which is why CKAN can emit
# `'restrict':14` even though position 13 (`no`) was removed.
_PG_ENGLISH_STOPWORDS: frozenset[str] = frozenset({
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
    "your", "yours", "yourself", "yourselves", "he", "him", "his",
    "himself", "she", "her", "hers", "herself", "it", "its", "itself",
    "they", "them", "their", "theirs", "themselves", "what", "which",
    "who", "whom", "this", "that", "these", "those", "am", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had", "having",
    "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if",
    "or", "because", "as", "until", "while", "of", "at", "by", "for",
    "with", "about", "against", "between", "into", "through", "during",
    "before", "after", "above", "below", "to", "from", "up", "down",
    "in", "out", "on", "off", "over", "under", "again", "further",
    "then", "once", "here", "there", "when", "where", "why", "how",
    "all", "any", "both", "each", "few", "more", "most", "other", "some",
    "such", "no", "nor", "not", "only", "own", "same", "so", "than",
    "too", "very", "s", "t", "can", "will", "just", "don", "should",
    "now",
})

# One shared stemmer instance (thread-safe for read-only `stemWord`
# calls — snowballstemmer's state is per-call, not per-instance).
_SNOWBALL = snowballstemmer.stemmer("english")

def build_legacy_pagination_links(
    url: str,
    *,
    limit: int,
    offset: int,
) -> dict[str, str]:
    """Legacy `_links` dict: path-only URL strings, no page counters.

    `start` is always present; `prev` when `offset > 0`; `next`
    whenever `limit > 0` (last page included).
    """
    parsed = urlparse(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    base_pairs = [(k, v) for k, v in pairs if k != "offset"]

    def _link(pairs: list[tuple[str, str]]) -> str:
        qs = urlencode(pairs)
        return f"{parsed.path}?{qs}" if qs else parsed.path

    out: dict[str, str] = {"start": _link(base_pairs)}
    if offset > 0:
        prev_offset = max(0, offset - limit)
        out["prev"] = _link([("offset", str(prev_offset))] + base_pairs)
    if limit > 0:
        out["next"] = _link([("offset", str(offset + limit))] + base_pairs)
    return out


def stream_legacy_search(
    *,
    help_url: str,
    resource_id: str,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    limit: int,
    offset: int,
    total: int | None,
    include_total: bool,
    links: dict[str, Any],
    records_format: str = "objects",
) -> Iterator[bytes]:
    """CKAN legacy `datastore_search` envelope writer.

    `records_format` picks the shape of the `records` field (default
    `objects`). The other three — `lists`, `csv`, `tsv` — match what
    the public CKAN API emits.
    """
    kept_fields, keep_indices = _project_fields(fields)
    columns = [f["id"] for f in kept_fields]

    yield b'{"help":'
    yield orjson.dumps(help_url)
    yield b',"success":true,"result":{'
    if include_total:
        yield b'"include_total":true,'
    yield b'"resource_id":'
    yield orjson.dumps(resource_id)
    yield b',"fields":'
    yield orjson.dumps(kept_fields)
    yield b',"records_format":'
    yield orjson.dumps(records_format)
    yield b',"records":'
    yield from _records_for_format(records_format, columns, records, keep_indices)
    yield b',"limit":'
    yield orjson.dumps(limit)
    yield b',"offset":'
    yield orjson.dumps(offset)
    yield b',"_links":'
    yield orjson.dumps(links)
    if include_total and total is not None:
        yield b',"total":'
        yield orjson.dumps(total)
    yield b"}"
    yield b"}"


def _records_for_format(
    records_format: str,
    columns: list[str],
    records: Iterator[tuple[Any, ...]],
    keep_indices: list[int],
) -> Iterator[bytes]:
    """Dispatch the `records` field writer by `records_format`.

    - `objects` → `[{col: v, ...}, ...]`
    - `lists`   → `[[v1, v2, ...], ...]`
    - `csv`     → JSON string of CSV data rows (no header; column names
                  on `result.fields`), matching real CKAN
    - `tsv`     → JSON string of TSV data rows (same)
    """
    if records_format == "objects":
        yield from _records_object_array(columns, records, keep_indices)
    elif records_format == "lists":
        yield from _records_array_array(records, keep_indices)
    elif records_format == "csv":
        yield from _records_delimited_string(records, keep_indices, delimiter=",")
    elif records_format == "tsv":
        yield from _records_delimited_string(records, keep_indices, delimiter="\t")
    else:
        yield from _records_object_array(columns, records, keep_indices)


def _records_array_array(
    records: Iterator[tuple[Any, ...]],
    keep_indices: list[int],
) -> Iterator[bytes]:
    """`[[v1, v2, ...], ...]` projected to kept columns."""
    yield b"["
    first = True
    for row in records:
        if first:
            first = False
        else:
            yield b","
        yield orjson.dumps(
            [row[i] for i in keep_indices],
            default=_json_default,
            option=orjson.OPT_PASSTHROUGH_DATETIME,
        )
    yield b"]"


def _records_delimited_string(
    records: Iterator[tuple[Any, ...]],
    keep_indices: list[int],
    *,
    delimiter: str,
) -> Iterator[bytes]:
    """One JSON string containing CSV / TSV data rows (no header).

    `_delimited_row` has to see post-serialisation primitives, so
    datetime / dict / list / bytes / Decimal cells are coerced via
    `_stringify` first — the dump writers do the same.
    """
    yield b'"'
    for row in records:
        projected = [_stringify(row[i]) for i in keep_indices]
        yield _json_string_inner(_delimited_row(projected, delimiter=delimiter))
    yield b'"'


def stream_legacy_search_sql(
    *,
    help_url: str,
    sql: str,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
) -> Iterator[bytes]:
    """CKAN legacy `datastore_search_sql` envelope writer.

    No `_links` (legacy SQL responses don't paginate) and no `limit` /
    `offset` echoes — the SQL carries them. `sql` is echoed back so
    callers can confirm what ran, matching real CKAN.

    No `records_truncated` key: real CKAN's `datastore_search_sql`
    doesn't emit one.

    `_full_text` is treated as a synthesised column: whenever the
    caller lists it in `fields` (right after `_id`, matching CKAN's
    `SELECT *` output), the writer computes a tsvector-shaped value
    per row from the row's other non-null cells. The engine never
    materialises it; the field list is the single source of truth.
    """
    # `_full_text` is virtual (synthesised per row). Pop it before
    # `_project_fields` runs so column→engine-row indices stay aligned,
    # then splice it back in with a sentinel index after projection.
    real_fields, full_text_at = _pop_full_text(fields)
    kept_fields, keep_indices = _project_fields(real_fields)
    if full_text_at is not None:
        slot = min(full_text_at, len(kept_fields))
        kept_fields = kept_fields[:slot] + [LEGACY_FULL_TEXT_FIELD] + kept_fields[slot:]
        keep_indices = keep_indices[:slot] + [-1] + keep_indices[slot:]
    columns = [f["id"] for f in kept_fields]

    yield b'{"help":'
    yield orjson.dumps(help_url)
    yield b',"success":true,"result":{"records":'
    if full_text_at is None:
        yield from _records_object_array(columns, records, keep_indices)
    else:
        yield from _records_with_full_text(columns, records, keep_indices, slot)
    yield b',"fields":'
    yield orjson.dumps(kept_fields)
    yield b',"sql":'
    yield orjson.dumps(sql.strip())
    yield b"}}"


def _pop_full_text(
    fields: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int | None]:
    """Strip the virtual `_full_text` entry from `fields` and return
    `(real_fields, original_index)`. Returns `(fields, None)` unchanged
    when `_full_text` is absent — the common case for every SQL shape
    other than `SELECT *`.
    """
    for i, f in enumerate(fields):
        if f.get("id") == "_full_text":
            return fields[:i] + fields[i + 1:], i
    return fields, None


def legacy_info_result(
    *,
    resource_id: str,
    fields: list[dict[str, Any]],
    meta: dict[str, Any],
) -> dict[str, Any]:
    """CKAN legacy `datastore_info` result body.

    No streaming — info responses are small. Returns the inner `result`
    dict ready to drop into `_success_response`'s envelope. Fields are
    stripped of `_id`/`_updated_at` and reduced to the minimal
    `{"type", "id"}` shape; meta is piped through verbatim.
    """
    kept_fields, _ = _project_fields(fields)
    out: dict[str, Any] = {
        "meta": meta,
        "fields": kept_fields,
    }
    if resource_id:
        out["resource_id"] = resource_id
    return out


def _records_with_full_text(
    columns: list[str],
    records: Iterator[tuple[Any, ...]],
    keep_indices: list[int],
    full_text_at: int,
) -> Iterator[bytes]:
    """`[{col: value, ...}, ...]` with a synthesised `_full_text` per row.

    `keep_indices[full_text_at] == -1` by construction (the writer
    splices a sentinel at the slot it got from `_full_text_slot`), so
    we substitute the computed value at that position. Every other
    position is a real engine-tuple index.

    `_full_text` is formatted as a Postgres tsvector lexeme string
    (`'word':pos1,pos2 'word2':pos3 ...`) built from the row's
    non-null values. We don't stem (Postgres uses English
    morphological stemming via `tsearch`) but we do lowercase,
    tokenise on non-alphanumerics, and 1-index positions in column
    order — which is close enough for clients that parse the shape.
    """
    yield b"["
    first = True
    for row in records:
        if first:
            first = False
        else:
            yield b","
        projected: list[Any] = []
        for pos, src_idx in enumerate(keep_indices):
            if pos == full_text_at:
                projected.append(None)  # placeholder — overwritten below
            else:
                projected.append(row[src_idx])
        projected[full_text_at] = _compute_full_text(
            [v for i, v in enumerate(projected) if i != full_text_at and columns[i] != "_id"]
        )
        yield orjson.dumps(
            dict(zip(columns, projected)),
            default=_json_default,
            option=orjson.OPT_PASSTHROUGH_DATETIME,
        )
    yield b"]"


# Token: an alphanumeric run, or a sign-prefixed numeric run. The
# sign prefix is intentional — Postgres's parser treats `-23` from
# `2024-09-23` as its own token (the leading dash is kept), matching
# the real CKAN sample where `'-09':2 '-23':3` appear as distinct
# lexemes. Trailing punctuation and spaces split; a lone `-` doesn't
# produce an empty token.
_FULL_TEXT_TOKEN_RE = re.compile(r"-?[A-Za-z0-9]+")


def _compute_full_text(values: list[Any]) -> str:
    """Build a Postgres-tsvector `_full_text` string, matching
    `to_tsvector('english', …)` byte-for-byte.

    Pipeline (mirrors Postgres's `english_stem` dictionary):

      1. Walk values in column order, stringify, tokenise.
      2. Lowercase each token and assign a 1-indexed position in walk
         order. **Positions advance for stopwords too** — they're
         counted, then dropped, so later tokens keep the index they
         would have had if the stopword had stayed. This is why the
         real CKAN sample has `'restrict':14` with no 13.
      3. Drop stopwords (Postgres's `english.stop` list).
      4. Stem each surviving token with Snowball English (`restrictions`
         → `restrict`, `networks` → `network`).
      5. Group same-stem positions, sort alphabetically by stem, emit
         `'stem':pos1,pos2 ...`.

    `_id` is excluded by the caller before invocation.
    """
    positions: dict[str, list[int]] = {}
    pos = 0
    for v in values:
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            text = orjson.dumps(v, default=_json_default).decode("utf-8")
        elif isinstance(v, bytes):
            text = v.decode("latin-1", errors="replace")
        else:
            text = str(v)
        for match in _FULL_TEXT_TOKEN_RE.finditer(text):
            token = match.group(0).lower()
            pos += 1  # advance even for stopwords
            if token in _PG_ENGLISH_STOPWORDS:
                continue
            stemmed = _SNOWBALL.stemWord(token)
            positions.setdefault(stemmed, []).append(pos)

    return " ".join(
        f"'{stem}':{','.join(str(p) for p in positions[stem])}"
        for stem in sorted(positions)
    )


def _project_fields(
    fields: list[dict[str, Any]],
    *,
    hidden: frozenset[str] = LEGACY_HIDDEN_COLUMNS,
) -> tuple[list[dict[str, Any]], list[int]]:
    """Reduce v2 `fields` to the CKAN-legacy shape, remembering which
    positions survived so row tuples can be sliced to match.

    Returns `(kept_fields, keep_indices)`. `kept_fields` entries carry
    `type`, `id`, and (when present) `info` — same key order the
    legacy CKAN API emits. Any other v2 extras are dropped.
    `keep_indices` is the list of original tuple positions to retain,
    in order.

    `info` is passed through so clients hitting `datastore_search` and
    `datastore_search_sql` see the same per-column metadata (title,
    description, unit, example, …) that `datastore_info` already
    returns. One schema update at the datastore_create / datastore_info
    layer now reaches every read endpoint without a second round-trip.

    `hidden` picks the column set to drop. Default
    `LEGACY_HIDDEN_COLUMNS` is the action-API set (just `_updated_at`);
    dump writers pass `LEGACY_DUMP_HIDDEN_COLUMNS` instead, which also
    drops `_id` to match CKAN's download layout.
    """
    kept_fields: list[dict[str, Any]] = []
    keep_indices: list[int] = []
    for i, f in enumerate(fields):
        fid = f.get("id")
        if fid in hidden:
            continue
        entry: dict[str, Any] = {}
        if "type" in f:
            entry["type"] = f["type"]
        if fid is not None:
            entry["id"] = fid
        info = f.get("info")
        if isinstance(info, dict) and info:
            entry["info"] = info
        kept_fields.append(entry)
        keep_indices.append(i)
    return kept_fields, keep_indices


def _records_object_array(
    columns: list[str],
    records: Iterator[tuple[Any, ...]],
    keep_indices: list[int],
) -> Iterator[bytes]:
    """`[{col: value, ...}, ...]` with each row projected to kept columns.

    `keep_indices` is applied inline so the hidden columns never reach
    `orjson.dumps`. Uses a per-row generator-expression tuple slice
    (tuples don't take an iterable of indices directly) which stays
    O(kept_columns) per row.
    """
    yield b"["
    first = True
    for row in records:
        if first:
            first = False
        else:
            yield b","
        projected = tuple(row[i] for i in keep_indices)
        yield orjson.dumps(
            dict(zip(columns, projected)),
            default=_json_default,
            option=orjson.OPT_PASSTHROUGH_DATETIME,
        )
    yield b"]"


# ---------------------------------------------------------------------------
# File-download writers for `/datastore/dump/{id}?format=…`
# ---------------------------------------------------------------------------
#
# One writer per CKAN-legacy format. All four yield raw bytes a chunk at
# a time so a 10M-row dump never leaves peak memory above ~1 row + any
# per-format buffer (constant-size `StringIO` for csv/tsv). They never
# emit the CKAN JSON envelope — these are file downloads, not actions.
#
# Shared contract:
#   fields          — v2 legacy-shape list `[{"id", "type", "info"?}, ...]`
#                     in engine column order. Projected to the kept subset
#                     via `_project_fields` first, so hidden columns never
#                     reach the writer.
#   records         — lazy tuple iterator, engine column order.
#   bom (csv/tsv/json/xml) — prepend UTF-8 BOM to the first chunk. CKAN's
#                     `?bom=true` for Excel; applied uniformly across
#                     formats because we don't second-guess the caller.


def dump_csv(
    *,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    bom: bool = False,
) -> Iterator[bytes]:
    """Stream a CSV file: header row + data rows, RFC 4180 quoting."""
    yield from _dump_delimited(fields=fields, records=records, delimiter=",", bom=bom)


def dump_tsv(
    *,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    bom: bool = False,
) -> Iterator[bytes]:
    """Stream a TSV file: header row + data rows, tab-delimited."""
    yield from _dump_delimited(fields=fields, records=records, delimiter="\t", bom=bom)


def dump_json(
    *,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    bom: bool = False,
) -> Iterator[bytes]:
    """Stream a JSON array of `{col: value, ...}` objects.

    Not NDJSON — CKAN's dump format is a single JSON array for
    compatibility with pandas / jq / standard JSON parsers. The
    streaming layout keeps the per-row cost identical to NDJSON.
    """
    kept_fields, keep_indices = _project_fields(fields, hidden=LEGACY_DUMP_HIDDEN_COLUMNS)
    columns = [f["id"] for f in kept_fields]
    if bom:
        yield UTF8_BOM
    yield from _records_object_array(columns, records, keep_indices)


def dump_xml(
    *,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    bom: bool = False,
) -> Iterator[bytes]:
    """Stream `<data><row><col>val</col>…</row>…</data>`.

    Matches CKAN's `<data>`-root / per-row `<row>` layout. Column values
    are rendered as text nodes; `None` becomes an empty element so the
    column still appears (so a consumer walking the XML by tag always
    finds every column). The declaration carries `encoding="UTF-8"`.
    """
    kept_fields, keep_indices = _project_fields(fields, hidden=LEGACY_DUMP_HIDDEN_COLUMNS)
    columns = [f["id"] for f in kept_fields]

    if bom:
        yield UTF8_BOM
    yield b'<?xml version="1.0" encoding="UTF-8"?>\n'
    yield b"<data>\n"
    for row in records:
        projected = tuple(row[i] for i in keep_indices)
        yield b"  <row>"
        for col, val in zip(columns, projected):
            tag = _xml_safe_tag(col)
            if val is None:
                yield f"<{tag}/>".encode("utf-8")
            else:
                yield f"<{tag}>{_xml_escape(_stringify(val))}</{tag}>".encode("utf-8")
        yield b"</row>\n"
    yield b"</data>\n"


# --- helpers for the dump writers -----------------------------------------


def _dump_delimited(
    *,
    fields: list[dict[str, Any]],
    records: Iterator[tuple[Any, ...]],
    delimiter: str,
    bom: bool,
) -> Iterator[bytes]:
    """Shared implementation for csv + tsv.

    Streams via a reused `StringIO` + `csv.writer` so quoting + escaping
    follow RFC 4180; the buffer is reset each row so memory stays O(1).
    Non-primitive values (dicts, lists, bytes, Decimal) are stringified
    through `_stringify` first — csv has no concept of a nested object.
    """
    kept_fields, keep_indices = _project_fields(fields, hidden=LEGACY_DUMP_HIDDEN_COLUMNS)
    columns = [f["id"] for f in kept_fields]

    buf = io.StringIO()
    writer = csv.writer(
        buf, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n"
    )

    def _emit_current_row() -> bytes:
        payload = buf.getvalue().encode("utf-8")
        buf.seek(0)
        buf.truncate(0)
        return payload

    if bom:
        yield UTF8_BOM

    writer.writerow(columns)
    yield _emit_current_row()

    for row in records:
        projected = [_stringify(row[i]) for i in keep_indices]
        writer.writerow(projected)
        yield _emit_current_row()


def _stringify(value: Any) -> str:
    """Coerce a scalar / Decimal / dict / list / bytes to a text form
    safe for a delimited cell or an XML text node.

    - `None` → `""` (csv has no `null`; XML path checks `None` separately).
    - dict / list → compact JSON (what CKAN does for its CSV dumps too).
    - bytes → latin-1 decode (lossless, bytewise); same intent as CKAN's
      fallback for `BYTEA` columns.
    - Decimal / numbers / bool → `str(value)` (bool as `True`/`False`
      matches CKAN's Python repr).
    - datetime / date / time → their ISO strings via `str()` are the
      shape BigQuery already hands us for text columns.
    """
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return orjson.dumps(value, default=_json_default).decode("utf-8")
    if isinstance(value, bytes):
        return value.decode("latin-1", errors="replace")
    return str(value)


# XML tag names must start with a letter / underscore and contain only
# letters, digits, hyphens, underscores and periods — anything else is
# rewritten to `_` so the output is always well-formed. Column ids go
# through `check_field_name` upstream (letters/digits/underscores +
# spaces), so the only character we actively rewrite is the space.
_XML_TAG_SAFE = str.maketrans({" ": "_"})


def _xml_safe_tag(name: str) -> str:
    """Turn a legacy field id into a valid XML element name."""
    safe = name.translate(_XML_TAG_SAFE)
    if not safe or not (safe[0].isalpha() or safe[0] == "_"):
        safe = "_" + safe
    return safe


# Minimal XML text-node escapes — ampersand first so later escapes aren't
# double-encoded, then the two angle brackets. We do NOT escape quotes
# because text nodes don't need it (XML only requires `&` and `<`; `>` is
# escaped by convention to avoid the `]]>` CDATA-end sequence).
def _xml_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
