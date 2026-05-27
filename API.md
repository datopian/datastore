# Datastore API Reference

A standalone, CKAN-compatible datastore service: tabular CRUD + search over a
pluggable storage backend. Every action lives under `/api/3/action/` and returns
the CKAN envelope, so existing CKAN datastore clients work unchanged — whether
this runs alongside CKAN or independently.

- **Interactive docs:** `GET /docs` (Swagger UI) · `GET /redoc` · `GET /openapi.json`
- **Postman:** import [postman/collection.json](postman/collection.json) — one worked request per endpoint.

---

## Conventions

### Response envelope

Every response is a CKAN envelope. On success:

```json
{ "help": "<request URL>", "success": true, "result": { ... } }
```

On failure:

```json
{
  "help": "<request URL>",
  "success": false,
  "error": {
    "__type": "Validation Error",
    "message": "human-readable explanation",
    "fields": { "field": ["..."] }
  }
}
```

`error.fields` is present only on validation errors. `null` values are never
serialised — absent fields are simply omitted.

### Error types

| `__type` | HTTP | When |
|---|---|---|
| `Validation Error` | 400 | Bad input — shape, types, unknown column, read-only resource |
| `Authorization Error` | 403 | Caller may not perform the action |
| `Not Found Error` | 404 | Resource not declared |
| `Conflict Error` | 409 | Unsupported in-place change (e.g. narrowing a column type) |
| `Internal Error` | 500 | Backend/transport failure |

### Authentication

Send the token in the **`Authorization`** header. The active provider is set by
`AUTH_TYPE`:

| `AUTH_TYPE` | Behaviour | Token |
|---|---|---|
| `ckan` | Delegates to CKAN `datastore_authorize` (TTL-cached) | CKAN API key |
| `jwt` | Verifies signature + `aud`/`iss`/`exp` locally | signed JWT |
| `anonymous` | Allows everything; no identity | none |

Read actions (`datastore_search`, `datastore_search_sql`, `datastore_info`) may
be attempted without a token; the provider decides. All write actions require a
token (except under `anonymous`).

### Endpoints at a glance

| Method | Path | Summary |
|---|---|---|
| POST | `/api/3/action/datastore_create` | Declare a resource (and optionally seed rows) |
| POST | `/api/3/action/datastore_upsert` | Insert / update / upsert rows |
| POST | `/api/3/action/datastore_delete` | Delete rows, drop columns, or drop the table |
| GET | `/api/3/action/datastore_search` | Search a resource (streaming) |
| GET | `/api/3/action/datastore_search_sql` | Run a read-only SQL `SELECT` (streaming) |
| GET | `/api/3/action/datastore_info` | Schema + row stats for a resource |
| GET | `/datastore/dump/{resource_id}` | Download a whole resource (CSV/NDJSON/Parquet) |
| GET | `/` · `/health` · `/ready` | Welcome / liveness / readiness |

---

## `POST /api/3/action/datastore_create`

Declare a resource (table) and optionally seed it with rows. Re-declaring an
existing resource adds columns and widens types (see below).

**Two input shapes:**

- `resource_id` — table name only. Works under any `AUTH_TYPE`.
- `resource` (dict) — creates a CKAN resource first (with `url_type="datastore"`),
  then writes the table. **`AUTH_TYPE=ckan` only**; rejected otherwise.

### Body

| Field | Type | Notes |
|---|---|---|
| `resource_id` | string | Target table. Provide this **or** `resource`. |
| `resource` | object | CKAN resource dict (ckan auth only). |
| `schema` | object | Frictionless Table Schema — the native column shape. |
| `fields` | array | *Deprecated* legacy `[{id, type, info}]`. Use `schema`. |
| `primary_key` | string \| array | *Deprecated*. Use `schema.primaryKey`. |
| `records` | array | Optional rows to seed. |
| `include_records` | bool | Echo written rows back in `result.records`. |
| `include_total` | bool | Run `COUNT(*)` and return `result.total`. |
| `force` | bool | Required to write a datastore-managed resource (see [read-only guard](#read-only-resource-guard)). |

**Field types** accept Frictionless canonical names (`integer`, `number`,
`string`, `boolean`, `date`, `datetime`, `time`, `object`, `array`, `geopoint`,
`geojson`, `any`) or SQL aliases (`int4`, `bigint`, `varchar`, `text`, `float`,
`numeric`, `bool`, `timestamp`, `json`, …), normalised to canonical on storage.
Each field may carry an `info` data dictionary (`title`, `description`,
`comment`, `example`, `unit`, plus custom keys), stored verbatim and
round-tripped by `datastore_info`.

### Request

```json
{
  "resource_id": "balancing_auction_results_2025",
  "schema": {
    "fields": [
      {"name": "auction_id", "type": "integer", "info": {"title": "Auction ID"}},
      {"name": "product_code", "type": "string"},
      {"name": "delivery_start", "type": "datetime"},
      {"name": "clearing_price_gbp_per_mwh", "type": "number", "info": {"unit": "GBP/MWh"}},
      {"name": "accepted", "type": "boolean"},
      {"name": "bidder_metadata", "type": "object"}
    ],
    "primaryKey": ["auction_id", "product_code"]
  },
  "records": [
    {"auction_id": 144, "product_code": "DCL", "delivery_start": "2025-11-04T16:00:00Z",
     "clearing_price_gbp_per_mwh": 47.82, "accepted": true,
     "bidder_metadata": {"unit_id": "DRAX-1"}}
  ]
}
```

### Response — 200

```json
{
  "help": "...",
  "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "fields": [{"id": "auction_id", "type": "integer", "info": {"...": "..."}}, "..."],
    "schema": {"fields": ["..."], "primaryKey": ["auction_id", "product_code"]},
    "primary_key": ["auction_id", "product_code"]
  }
}
```

`records` and `total` appear only when `include_records` / `include_total` are set.

---

## `POST /api/3/action/datastore_upsert`

Write rows into an existing resource (declare it with `datastore_create` first).

### Body

| Field | Type | Notes |
|---|---|---|
| `resource_id` | string | Target table (required). |
| `records` | array | Rows to write. |
| `method` | string | `upsert` (default) · `insert` · `update`. |
| `include_records` | bool | Echo written rows in `result.records`. |
| `include_total` | bool | Return `result.total`. |
| `force` | bool | Required for a datastore-managed resource (see [read-only guard](#read-only-resource-guard)). |

- **`upsert`** — `MERGE` on the table's stored `primaryKey`: match → update, miss → insert.
- **`insert`** — append rows; no key check.
- **`update`** — every row must match an existing key, else `Not Found Error`.

The table's `unique_key` (set at create) decides matching — the request body
never carries it.

### Request

```json
{
  "resource_id": "balancing_auction_results_2025",
  "method": "upsert",
  "records": [
    {"auction_id": 144, "product_code": "DCL", "clearing_price_gbp_per_mwh": 48.05, "accepted": true}
  ]
}
```

### Response — 200

```json
{ "help": "...", "success": true,
  "result": {"resource_id": "balancing_auction_results_2025", "method": "upsert"} }
```

---

## `POST /api/3/action/datastore_delete`

Three modes (`filters` and `fields` are mutually exclusive):

- **Drop table** — omit both `filters` and `fields`.
- **Delete rows** — `filters` (only rows matching every `column: value`).
- **Drop columns** — `fields` (list of column names).

### Body

| Field | Type | Notes |
|---|---|---|
| `resource_id` / `id` | string | Target table (one required; `id` is a CKAN alias). |
| `filters` | object | Row filter. Omit (with no `fields`) → drop the table. |
| `fields` | array | Columns to drop. Mutually exclusive with `filters`. |
| `force` | bool | Required for a datastore-managed resource (see [read-only guard](#read-only-resource-guard)). |

### Request

```json
{ "resource_id": "balancing_auction_results_2025",
  "filters": {"auction_id": 144, "accepted": false} }
```

### Response — 200

```json
{ "help": "...", "success": true,
  "result": {"resource_id": "balancing_auction_results_2025"} }
```

On a **column drop**, `result` also carries `schema` — the Frictionless schema
after the columns were removed — so you can confirm the new shape without a
follow-up `datastore_info`:

```json
{ "help": "...", "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "fields": ["bidder_metadata"],
    "schema": {"fields": [{"name": "auction_id", "type": "integer"}, "..."],
               "primaryKey": ["auction_id", "product_code"]}
  } }
```

---

## `GET /api/3/action/datastore_search`

Parameterised search; the response is **streamed** (peak memory ≈ one row).

### Query parameters

| Name | Type | Default | Notes |
|---|---|---|---|
| `resource_id` | string | — | required |
| `filters` | JSON object | — | `{"col": value}` or `{"col": [v1, v2]}` (IN) |
| `q` | string \| JSON | — | full-text (string = all columns; object = per column) |
| `distinct` | bool | `false` | |
| `plain` | bool | `true` | reserved (CKAN-compat) |
| `language` | string | `"english"` | reserved (CKAN-compat) |
| `limit` | int | `100` | capped by `SEARCH_RESULT_ROWS_MAX` |
| `offset` | int | `0` | |
| `fields` | CSV | all | comma-separated columns to project |
| `sort` | string | — | `"col asc, col2 desc"` |
| `include_total` | bool | `true` | runs `COUNT(*)` when needed |
| `records_format` | string | `"objects"` | `objects` · `lists` · `csv` · `tsv` |

### Example

```
GET /api/3/action/datastore_search
    ?resource_id=balancing_auction_results_2025
    &filters={"product_code":"DCL","accepted":true}
    &sort=delivery_start desc
    &limit=100
```

### Response (records_format=objects)

```json
{
  "help": "...",
  "success": true,
  "result": {
    "fields": [{"id": "auction_id", "type": "integer"}, "..."],
    "records": [
      {"auction_id": 144, "product_code": "DCL", "clearing_price_gbp_per_mwh": 47.82}
    ],
    "total": 2,
    "_links": {
      "start": ".../datastore_search?resource_id=...&limit=100",
      "next":  ".../datastore_search?resource_id=...&limit=100&offset=100"
    }
  }
}
```

- `records_format=lists` → each record is a positional array (column order = `fields`).
- `records_format=csv` / `tsv` → `records` is a single text body (header row first),
  still inside the JSON envelope.
- Paginate by following `_links.next`; end-of-data is an empty `records` array.

---

## `GET /api/3/action/datastore_search_sql`

Run a single read-only `SELECT` / `WITH` statement and stream the result. Tables
are referenced by `resource_id`; each is authorized individually, and functions
are checked against the engine's allow-list. Include a `LIMIT` (required).

### Query parameters

| Name | Type | Notes |
|---|---|---|
| `sql` | string | A single `SELECT`/`WITH`; no multi-statement, no DML/DDL. |

### Example

```
GET /api/3/action/datastore_search_sql?sql=
  SELECT product_code, AVG(clearing_price_gbp_per_mwh) AS avg_price
  FROM "balancing_auction_results_2025"
  WHERE accepted = true
  GROUP BY product_code
  LIMIT 1000
```

### Response

```json
{
  "help": "...",
  "success": true,
  "result": {
    "fields": [{"id": "product_code", "type": "string"}, {"id": "avg_price", "type": "number"}],
    "records": [{"product_code": "DCL", "avg_price": 47.82}]
  }
}
```

Safety: the schema rejects non-`SELECT` / multi-statement / unparseable SQL;
the load-bearing guard is a **read-only** backend credential that physically
refuses DML/DDL.

---

## `GET /api/3/action/datastore_info`

Returns the column schema (including the `info` data dictionary, verbatim) plus
row stats — a column-level metadata catalog without a side store.

### Query parameters

| Name | Type | Notes |
|---|---|---|
| `resource_id` / `id` | string | One required (`id` is a CKAN alias). |

### Response

```json
{
  "help": "...",
  "success": true,
  "result": {
    "fields": [
      {"id": "auction_id", "type": "integer",
       "info": {"title": "Auction ID", "comment": "MANDATORY"}}
    ],
    "schema": {"fields": ["..."], "primaryKey": ["auction_id", "product_code"]},
    "meta": {
      "resource_id": "balancing_auction_results_2025",
      "primary_key": ["auction_id", "product_code"],
      "total": 18420
    }
  }
}
```

---

## `GET /datastore/dump/{resource_id}`

Download an entire resource. Pick the format with `?format=csv` (default),
`ndjson`, or `parquet`.

- **Small export (single shard):** `302` redirect to a signed GCS URL (bytes go
  straight from storage to the client).
- **Large export (multiple shards, CSV/NDJSON):** a streamed body
  (`200`, ~constant memory).
- Parquet over the single-shard limit returns `413` — switch to CSV/NDJSON.

Requires `read` permission on the resource and a configured export bucket
(`BIGQUERY_EXPORT_BUCKET`).

---

## Health

All return the CKAN envelope.

| Method | Path | Result |
|---|---|---|
| GET | `/` | `{"message": "<APP_MESSAGE>"}` |
| GET | `/health` | `{"status": "ok"}` — liveness; always 200 while the process runs |
| GET | `/ready` | `{"status": "ready"}` — 200 when both engines pass `healthcheck()`; `503` (`{"status": "not_ready"}`) otherwise |

---

## Read-only resource guard

Under **`AUTH_TYPE=ckan`**, `datastore_create`, `datastore_upsert`, and
`datastore_delete` refuse to write a resource whose CKAN record carries
`url_type="datastore"` unless the request sets `force: true`:

```json
{ "help": "...", "success": false,
  "error": {"__type": "Validation Error",
            "message": "Cannot update a read-only resource. Use \"force\" to force update."} }
```

This mirrors CKAN's protection against clobbering datastore-managed data. It is
gated on `AUTH_TYPE=ckan` and skipped entirely under other providers.
