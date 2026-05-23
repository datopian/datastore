"""Build a Postman v2.1.0 collection from `example_payload/`.

Walks every `example_payload/<endpoint>/*.json`, decides POST-vs-GET
from the endpoint name, and emits a request per example. POST bodies
carry the JSON verbatim; GET endpoints unfold the top-level keys into
URL query params (matching how the API actually consumes them).

Run from the repo root:

    python postman/generate_postman.py

Output: `postman/collection.json` — import into Postman / Insomnia and
set the `apiKey` collection variable.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

REPO = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO / "example_payload"
OUT_FILE = REPO / "postman" / "collection.json"

# Each endpoint's HTTP method + folder description. Order here matches
# the walkthrough flow: declare → write → inspect → query → cleanup.
# Run requests top-to-bottom on a fresh resource to see the full chain.
ENDPOINTS: list[tuple[str, str, str]] = [
    (
        "datastore_create", "POST",
        "Declare a resource and optionally seed it with rows. Run this "
        "first. Accepts either the canonical Frictionless `schema` or "
        "the legacy `fields` + `primary_key` shape.",
    ),
    (
        "datastore_upsert", "POST",
        "Write rows. `method` picks the strategy: `upsert` (default — "
        "match by primaryKey, insert new), `insert` (fail on duplicate), "
        "or `update` (fail on miss). Run after `datastore_create`.",
    ),
    (
        "datastore_info", "GET",
        "Read the resource's column schema + row count. Useful for "
        "confirming the table exists and verifying writes landed.",
    ),
    (
        "datastore_search", "GET",
        "Stream rows matching `filters` / `q`. Pagination via "
        "`limit` + `offset`; response carries `_links.next` / `prev` / "
        "`page_size` / `page` / `total_pages`.",
    ),
    (
        "datastore_search_sql", "GET",
        "Run a vetted `SELECT` / `WITH` statement. `LIMIT` is required "
        "(parsed from the SQL); pagination links rewrite the SQL's "
        "`OFFSET` so callers can follow `next` without editing.",
    ),
    (
        "datastore_delete", "POST",
        "Cleanup. Drop the table (no filters/fields), delete rows "
        "(`filters`), or drop columns (`fields`). `filters` and "
        "`fields` are mutually exclusive.",
    ),
]

HEALTH_REQUESTS: list[tuple[str, str, str]] = [
    ("Welcome",  "",       "Banner / root endpoint. Echoes `APP_MESSAGE`."),
    ("Health",   "health", "Liveness probe — always 200 while the process is up."),
    ("Ready",    "ready",  "Readiness probe — 200 only when both engines pass "
                           "healthcheck."),
]


def _request_url(path: str, query: list[dict[str, str]] | None = None) -> dict[str, Any]:
    """Postman v2.1 structured URL — lets the Postman UI edit params."""
    parts = path.strip("/").split("/")
    # Values can be JSON-encoded (e.g. `filters={"col":"v"}`) or contain
    # spaces / `=` / `&`; percent-encode so the `raw` URL parses cleanly.
    url: dict[str, Any] = {
        "raw": "{{baseUrl}}/" + "/".join(parts) + (
            "?" + "&".join(
                f"{quote(q['key'], safe='')}={quote(q['value'], safe='')}"
                for q in query
            )
            if query else ""
        ),
        "host": ["{{baseUrl}}"],
        "path": parts,
    }
    if query:
        url["query"] = query
    return url


def _post_request(action: str, body: dict[str, Any], description: str) -> dict[str, Any]:
    return {
        "method": "POST",
        "header": [{"key": "Content-Type", "value": "application/json"}],
        "body": {
            "mode": "raw",
            "raw": json.dumps(body, indent=2),
            "options": {"raw": {"language": "json"}},
        },
        "url": _request_url(f"api/3/action/{action}"),
        "description": description,
    }


def _get_request(action: str, body: dict[str, Any], description: str) -> dict[str, Any]:
    """Each top-level key of the JSON becomes a query-string param."""
    query: list[dict[str, str]] = []
    for key, value in body.items():
        if value is None:
            continue
        if isinstance(value, (dict, list)):
            # GET endpoints accept these as JSON-encoded strings on the
            # URL — see `to_json_object` / `to_csv_list` in validators.
            string_value = json.dumps(value, separators=(",", ":"))
        else:
            string_value = str(value).lower() if isinstance(value, bool) else str(value)
        query.append({"key": key, "value": string_value})
    return {
        "method": "GET",
        "header": [],
        "url": _request_url(f"api/3/action/{action}", query=query),
        "description": description,
    }


# Friendly request names + descriptions for each example payload. Falls
# back to a generated string when a file isn't listed here. Keys are
# `<action>/<stem>` so the same stem can mean different things across
# endpoints (e.g. `with_filters` in search vs delete).
SCENARIOS: dict[str, tuple[str, str]] = {
    "datastore_create/with_schema": (
        "Create - Frictionless schema (recommended)",
        "Canonical input: pass a Frictionless Table Schema under "
        "`schema` and (optionally) seed rows under `records`.",
    ),
    "datastore_create/with_resource_id": (
        "Create - legacy fields, existing resource",
        "Back-compat path: the legacy `fields` + `primary_key` shape "
        "against a resource id that already exists in CKAN.",
    ),
    "datastore_create/with_resource": (
        "Create - legacy fields, new resource",
        "Back-compat path: declare a brand-new CKAN resource inline "
        "with `resource: {…}` plus legacy `fields` / `primary_key`.",
    ),
    "datastore_upsert/upsert": (
        "Upsert - method=upsert (default)",
        "Match each row by stored `primaryKey`; update on hit, insert "
        "on miss. Updates only bump `_updated_at` when a non-PK column "
        "actually changes.",
    ),
    "datastore_upsert/insert": (
        "Upsert - method=insert",
        "Insert only; duplicate primary key surfaces as a clean "
        "ValidationError.",
    ),
    "datastore_upsert/update": (
        "Upsert - method=update",
        "Update only; any row whose `primaryKey` doesn't match an "
        "existing row raises NotFoundError.",
    ),
    "datastore_info/basic": (
        "Info - by resource_id",
        "Return the stored Frictionless schema + row count via "
        "`INFORMATION_SCHEMA.TABLE_STORAGE`.",
    ),
    "datastore_info/with_id_alias": (
        "Info - `id` alias",
        "`id` is accepted as a legacy CKAN alias for `resource_id`.",
    ),
    "datastore_search/basic": (
        "Search - minimal (just resource_id)",
        "Default page (limit=100, offset=0, include_total=true).",
    ),
    "datastore_search/with_filters": (
        "Search - with filters",
        "JSON-encoded filters on the URL. Value matches must respect "
        "column types (no JSON-column equality).",
    ),
    "datastore_search/with_full_text": (
        "Search - `q` full-text",
        "BigQuery `SEARCH(row, @q)` against every text column.",
    ),
    "datastore_search/paginated_sorted": (
        "Search - paginated + sorted",
        "Custom projection (CSV), multi-column sort, explicit limit / "
        "offset / include_total. Drives `_links.next`.",
    ),
    "datastore_search_sql/basic": (
        "SQL - basic SELECT",
        "Plain SELECT with WHERE + LIMIT. Total comes from "
        "`INFORMATION_SCHEMA.TABLE_STORAGE` since there's no aggregate.",
    ),
    "datastore_search_sql/aggregate": (
        "SQL - GROUP BY + aggregates",
        "Aggregates collapse rows, so total goes through the "
        "`COUNT(*) FROM (<inner>)` path instead of the metadata shortcut.",
    ),
    "datastore_search_sql/with_cte": (
        "SQL - WITH (CTE)",
        "Common table expression. CTE aliases are excluded from auth + "
        "qualification (they're inline, not external tables).",
    ),
    "datastore_search_sql/paginated": (
        "SQL - LIMIT + OFFSET",
        "Pagination via OFFSET. `_links.next` will rewrite the OFFSET "
        "in the SQL string so the caller can follow without editing.",
    ),
    "datastore_search_sql/join": (
        "SQL - JOIN two resources",
        "JOIN across two resource_ids. Each table is authorised "
        "independently via CKAN; both get the `project.dataset` prefix.",
    ),
    "datastore_search_sql/union": (
        "SQL - UNION ALL two resources",
        "UNION ALL across two resource_ids — handy for combined "
        "reports over time-partitioned tables.",
    ),
    "datastore_delete/whole_table": (
        "Delete - drop the whole table",
        "No `filters`, no `fields` → DROP TABLE + delete metadata row. "
        "Resource disappears entirely.",
    ),
    "datastore_delete/with_filters": (
        "Delete - narrow row delete",
        "Parameterised `DELETE FROM … WHERE …` against the filter "
        "columns. JSON-column equality rejected at the boundary.",
    ),
    "datastore_delete/with_fields": (
        "Delete - drop columns",
        "`ALTER TABLE DROP COLUMN …` + rewrite the stored schema. "
        "System columns (`_id`, `_updated_at`) and PK columns are "
        "protected.",
    ),
    "datastore_delete/force_readonly": (
        "Delete - force read-only resource",
        "`force=true` bypasses the CKAN read-only guard.",
    ),
}


# Preferred request order within each folder. Items not listed here
# fall in at the end, alphabetically.
SCENARIO_ORDER: dict[str, list[str]] = {
    "datastore_create": [
        "with_schema", "with_resource_id", "with_resource",
    ],
    "datastore_upsert": [
        "upsert", "insert", "update",
    ],
    "datastore_info": [
        "basic", "with_id_alias",
    ],
    "datastore_search": [
        "basic", "with_filters", "with_full_text", "paginated_sorted",
    ],
    "datastore_search_sql": [
        "basic", "aggregate", "with_cte",
        "paginated", "join", "union",
    ],
    "datastore_delete": [
        "whole_table", "with_filters", "with_fields", "force_readonly",
    ],
}


def _sorted_payloads(action: str, dir_path: Path) -> list[Path]:
    """Order example files by `SCENARIO_ORDER` (intro → advanced); fall
    back to filename for anything not pre-listed."""
    preferred = SCENARIO_ORDER.get(action, [])
    rank = {stem: i for i, stem in enumerate(preferred)}
    files = [p for p in dir_path.iterdir() if p.suffix == ".json"]
    files.sort(key=lambda p: (rank.get(p.stem, 10_000), p.stem))
    return files


def _scenario(action: str, payload_file: Path) -> tuple[str, str]:
    """Friendly name + description for a request."""
    key = f"{action}/{payload_file.stem}"
    if key in SCENARIOS:
        return SCENARIOS[key]
    # Fallback for new examples not yet in the lookup.
    name = payload_file.stem.replace("_", " ")
    rel = payload_file.relative_to(REPO).as_posix()
    return name, f"Scenario from {rel}."


def _build_endpoint_folder(
    action: str, method: str, description: str,
) -> dict[str, Any]:
    folder_items: list[dict[str, Any]] = []
    dir_path = SOURCE_DIR / action
    if not dir_path.is_dir():
        return {"name": action, "description": description, "item": []}
    for payload_file in _sorted_payloads(action, dir_path):
        if "response" in payload_file.stem:
            # Sample server responses live next to requests for docs;
            # they don't belong in a Postman *request* collection.
            continue
        with payload_file.open() as f:
            body = json.load(f)
        name, scenario_desc = _scenario(action, payload_file)
        builder = _post_request if method == "POST" else _get_request
        folder_items.append({
            "name": name,
            "request": builder(action, body, scenario_desc),
            "response": [],
        })
    return {
        "name": action,
        "description": description,
        "item": folder_items,
    }


def _build_health_folder() -> dict[str, Any]:
    items = []
    for name, sub, desc in HEALTH_REQUESTS:
        path = sub if sub else ""
        items.append({
            "name": name,
            "request": {
                "method": "GET",
                "header": [],
                "url": _request_url(path),
                "description": desc,
            },
            "response": [],
        })
    return {
        "name": "health",
        "description": (
            "Health endpoints live at the root and also under "
            "`/api/3/action/` so k8s probes and CKAN clients can both "
            "reach them. Listed here at the root."
        ),
        "item": items,
    }


def build_collection() -> dict[str, Any]:
    folders: list[dict[str, Any]] = [_build_health_folder()]
    for action, method, description in ENDPOINTS:
        folders.append(_build_endpoint_folder(action, method, description))
    return {
        "info": {
            "_postman_id": str(uuid.uuid4()),
            "name": "Datastore API",
            "description": (
                "CKAN-compatible datastore API — auto-generated from "
                "`example_payload/`. Set `baseUrl` to your server, "
                "`apiKey` to a CKAN API key (anonymous reads are "
                "allowed; writes require a key), and `resourceId` to "
                "the table you want to hit."
            ),
            "schema": (
                "https://schema.getpostman.com/json/collection/"
                "v2.1.0/collection.json"
            ),
        },
        "variable": [
            {"key": "baseUrl", "value": "http://localhost:8000", "type": "string"},
            {"key": "apiKey",  "value": "", "type": "string"},
            {"key": "resourceId", "value": "balancing_auction_results_2025",
             "type": "string"},
        ],
        "auth": {
            "type": "apikey",
            "apikey": [
                {"key": "key",   "value": "Authorization", "type": "string"},
                {"key": "value", "value": "{{apiKey}}",     "type": "string"},
                {"key": "in",    "value": "header",        "type": "string"},
            ],
        },
        "item": folders,
    }


def main() -> None:
    collection = build_collection()
    OUT_FILE.parent.mkdir(exist_ok=True)
    with OUT_FILE.open("w") as f:
        json.dump(collection, f, indent=2)
        f.write("\n")
    request_count = sum(len(folder["item"]) for folder in collection["item"])
    print(f"Wrote {OUT_FILE.relative_to(REPO)} with {request_count} requests.")


if __name__ == "__main__":
    main()
