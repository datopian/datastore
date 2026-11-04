"""Legacy CKAN datastore API — DEPRECATED.

================================================================
  WHY THIS PACKAGE EXISTS
================================================================

This whole subtree is a backwards-compatibility shim for callers who
hit the public CKAN datastore URLs — `/api/3/action/datastore_*` and
`/datastore/dump/{resource_id}`. It is not part of the v2 contract and
it is scheduled for removal.

Everything here will be deleted — the folder in one shot, nothing from
it imported elsewhere — once customers migrate off the legacy URLs.
Do not add new features here; new work lives in the v2 surface
(`datastore/api/`, `datastore/services/`, `datastore/schemas/`).

================================================================
  LAYOUT
================================================================

    legacy/
    ├── __init__.py     # this file — package docstring + banner helper
    ├── constants.py    # URL prefixes, dump formats, hidden columns
    ├── schemas.py      # legacy-only Pydantic subclasses
    ├── streaming.py    # envelope + dump byte writers (pure Python)
    └── endpoints.py    # FastAPI routers (the ONE fastapi-aware module)

Legacy follows the same one-way layering v2 uses:
    endpoints.py  →  streaming.py  →  (plain data)

Only `endpoints.py` imports from `fastapi`/`starlette`. The layer-
invariant grep in CI continues to pass as-is.

================================================================
  WHAT LEGACY ALTERS vs. v2
================================================================

- Response envelope: no `schema`, no `records_format` beyond
  `objects`, no `warnings`.
- `fields` entries are the minimal `{"type", "id"}` shape; v2's
  `info` dictionary is dropped.
- Engine-managed `_id` / `_updated_at` columns are stripped from both
  `fields` and `records`.
- `datastore_search_sql` accepts SQL without a LIMIT clause (v2
  requires one).
- Dump supports CSV / TSV / JSON (array) / XML and the full
  `datastore_search` filter param set, plus `bom=true` for Excel.

================================================================
  REMOVAL PLAN
================================================================

When no customer traffic is observed on the legacy URLs for an agreed
window, run these steps in order. Each step is independent — the app
boots after any one of them, so the removal can be split across PRs.

  1. **routes.py** — remove the three lines in `datastore/api/routes.py`
     that mount `legacy_endpoints.router` and `legacy_endpoints.dump_router`
     (search for the `DEPRECATED legacy CKAN surface` block).
  2. **main.py** — remove the `log_startup_banner` import at the top and
     the `_log_legacy_banner()` call in the lifespan (both marked
     `DEPRECATED legacy surface`).
  3. **analytics.py** — remove the `from datastore.legacy.constants`
     import and every block marked `LEGACY ONLY` in
     `datastore/analytics.py`. Three sites total: the two prefix
     constants near the top, the two `if path.startswith(LEGACY_*)`
     branches in `action_name`, the `LEGACY_ACTION_PREFIX` entry in
     the POST-capture tuple, and the `LEGACY_DUMP_PATH_PREFIX` entry
     in `_resource_ref`'s prefix loop.
  4. **tests/test_legacy_api.py** — delete the file.
  5. **datastore/legacy/** — `rm -rf` the whole folder.
  6. **pyproject.toml** — drop the `snowballstemmer` dependency; it
     exists only for byte-for-byte `_full_text` tsvector parity.

Verification: `rg "datastore\.legacy|/api/3/action|/datastore/dump"
datastore/ tests/` must return nothing after the above. Then run
`pytest` + `ruff check` to confirm no stale references leak through.

Nothing in v2 imports from this package. The removal cannot break the
v2 surface because every touch point outside this folder is marked
with either `DEPRECATED` (file-level) or `LEGACY ONLY` (line-level).
"""

from __future__ import annotations

import logging

from datastore.legacy.constants import LEGACY_API_PREFIX, LEGACY_DUMP_PREFIX

_log = logging.getLogger(__name__)


def log_startup_banner() -> None:
    """Emit a WARNING at app boot so the deprecated surface is obvious
    in the first page of logs — matches `main.py`'s own ready-log tone.

    Called from `create_app()` after the engines log their state. One
    log line per process start; no per-request overhead.
    """
    _log.warning(
        "LEGACY CKAN routes mounted (deprecated, pending removal): "
        "%s/datastore_{search,search_sql,info}  %s/{resource_id}",
        LEGACY_API_PREFIX,
        LEGACY_DUMP_PREFIX,
    )
