"""Constants used ONLY by the legacy CKAN compatibility surface.

Kept in a legacy-local module (not `datastore/core/constants.py`) so
anyone grepping core/ for live config doesn't trip over values that are
about to be removed, and so the removal step is `rm -rf datastore/legacy`
with no scattered follow-ups.

DEPRECATED — see `datastore/legacy/__init__.py` for the removal plan.
"""

from __future__ import annotations

from typing import Literal

# Legacy CKAN URL prefixes. Mounted so scripts hitting the legacy CKAN
# datastore (`/api/3/action/...` and `/datastore/dump/...`) keep working
# byte-for-byte while callers migrate to v2.
LEGACY_API_PREFIX = "/api/3/action"
LEGACY_DUMP_PREFIX = "/datastore/dump"

# Engine-managed columns the legacy response must NOT expose on the
# JSON action API (`datastore_search`, `datastore_search_sql`,
# `datastore_info`). `_updated_at` is datastore-internal (last-write
# timestamp used by the dump cache invalidation path), and real CKAN
# never surfaces it to clients. `_id` is kept here — real CKAN exposes
# it as a stable row id that scripts in the wild filter and sort by
# (`?sort=_id asc`).
LEGACY_HIDDEN_COLUMNS: frozenset[str] = frozenset({"_updated_at"})

# The dump endpoint (`/datastore/dump/{id}?format=…`) hides one more
# column than the action API: `_id`. Real CKAN's dump omits it because
# a download is meant to look like the "clean" dataset a publisher
# would hand over — the datastore's row-id serial is an implementation
# detail that shouldn't leak into CSVs people archive and share.
LEGACY_DUMP_HIDDEN_COLUMNS: frozenset[str] = LEGACY_HIDDEN_COLUMNS | {"_id"}

# CKAN synthesises a `_full_text` column on every Postgres-backed
# datastore table (a `tsvector` built from the row's text columns).
# BigQuery has no equivalent, so we compute an approximation at
# response time: a space-joined concatenation of the row's non-null
# text values. Callers that filter on `?q=` are driven by the engine's
# own search index — this synthesised column is for display parity
# only, so a client script that reads `record["_full_text"]` sees a
# value in the shape it expects. Emitted on `datastore_search` only;
# `datastore_search_sql` echoes the user's SELECT shape verbatim.
LEGACY_FULL_TEXT_FIELD: dict[str, str] = {"type": "tsvector", "id": "_full_text"}

# Legacy CKAN dump formats. Mirrors what CKAN 2.9 serves at
# `/datastore/dump/{id}?format=…` (see §9 of the CKAN maintainer docs).
# All four stream API-side — no GCS redirect — because tsv/json/xml have
# no engine-native export and even csv needs row-level filtering (hidden
# system columns, user `filters` / `q`), which the GCS export pipeline
# does not do. Keeping one code path across formats is the win.
LegacyDumpFormat = Literal["csv", "tsv", "json", "xml"]
LEGACY_DUMP_FORMATS: tuple[str, ...] = ("csv", "tsv", "json", "xml")

LEGACY_DUMP_MEDIA_TYPES: dict[str, str] = {
    "csv": "text/csv; charset=utf-8",
    "tsv": "text/tab-separated-values; charset=utf-8",
    "json": "application/json",
    "xml": "application/xml; charset=utf-8",
}

LEGACY_DUMP_EXTENSIONS: dict[str, str] = {
    "csv": "csv",
    "tsv": "tsv",
    "json": "json",
    "xml": "xml",
}

# UTF-8 byte-order mark. CKAN's `?bom=true` prepends it so Excel on
# Windows opens the file as UTF-8 instead of mis-decoding it as cp1252.
# Only meaningful for the text formats; `?bom=true&format=json`/`xml`
# still prepends — matching CKAN rather than second-guessing callers.
UTF8_BOM = b"\xef\xbb\xbf"
