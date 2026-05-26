# Datastore API

A CKAN-shaped action API for tabular data storage and querying, built
on FastAPI with **two pluggable axes**:

- **Storage engine** — `DATASTORE_ENGINE` selects a folder under
  `datastore/infrastructure/engines/` (BigQuery today; DuckLake planned).
- **Auth provider** — `AUTH_TYPE` selects a folder under `datastore/auth/`.
  Built-in: `ckan` (delegates to an upstream CKAN, TTL-cached),
  `jwt` (verifies signature + claims locally), `anonymous` (allow-all,
  for local dev / CI).

Exposes `/api/3/action/datastore_*` endpoints. Runs **standalone**
under `AUTH_TYPE=anonymous` or `AUTH_TYPE=jwt` — no CKAN required —
or as a satellite to CKAN under `AUTH_TYPE=ckan`, in which case CKAN
remains the single source of truth for users, packages, resources,
and permissions, and the heavy datastore work lives here.

## Project structure

```
datastore/
├── main.py                       # FastAPI app factory + lifespan
│
├── api/                          # HTTP layer — only layer that imports fastapi / starlette
│   ├── routes.py                 # Top-level APIRouter; aggregates endpoints/
│   ├── context.py                # RequestContext (per-request DI bundle: config,
│   │                             # api_key, auth_provider, ckan); .authorize() method
│   ├── auth.py                   # Boundary policy (permission whitelist + anonymous-read
│   │                             # rule); delegates to the active AuthProvider
│   ├── middleware.py             # ASGI middleware (e.g. BodySizeLimitMiddleware)
│   ├── responses.py              # Envelope response helpers (_success_response / _error_response)
│   ├── error_handlers.py         # Exception handlers (APIError → CKAN error envelope)
│   └── endpoints/                # Route handlers, one file per resource group
│       ├── health.py             # /, /health, /ready
│       ├── datastore.py          # /api/3/action/datastore_*
│       └── dump.py               # /datastore/dump/<resource_id> (302 single / stream multi)
│
├── auth/                         # Pluggable auth providers — one subpackage per type
│   ├── base.py                   # AuthProvider Protocol + Decision dataclass +
│   │                             # default_key_id (JWT jti / sha256 helper)
│   ├── registry.py               # get_auth_provider(config, **extras) — importlib dispatch
│   ├── ckan/                     # AUTH_TYPE=ckan: calls /api/3/action/datastore_authorize
│   │                             # via CKANClient; holds its own TTL cache (the only
│   │                             # network-bound provider) so we don't hit CKAN per request
│   ├── jwt/                      # AUTH_TYPE=jwt: verifies HS*/RS*/ES* signature + aud/iss
│   └── anonymous/                # AUTH_TYPE=anonymous: always allows; no identity
│
├── core/                         # Cross-cutting helpers — no I/O, no fastapi
│   ├── config.py                 # Pydantic-Settings `Config` (env-driven) + get_config()
│   ├── constants.py              # Shared constants (type maps, defaults, …)
│   ├── exceptions.py             # APIError taxonomy + HTTP status → label map
│   └── helper.py                 # Pure helpers (e.g. parse_authorization_header)
│
├── schemas/                      # Pydantic request/response shapes (boundary validation only)
│   ├── request.py                # Inbound request models (DatastoreCreateRequest, …)
│   ├── responses.py              # Outbound CKAN envelopes (ResponseModel + per-endpoint)
│   └── validators.py             # Reusable Annotated types + field validators
│
├── services/                     # Business logic
│   ├── write.py                  # create / upsert / delete orchestration
│   ├── read.py                   # search / search_sql orchestration (engine call,
│   │                             # format dispatch, pagination links)
│   ├── streaming.py              # per-format byte-yielding writers used by read.py
│   └── dump.py                   # multi-shard stream-concat over async httpx
│                                 # (drives /datastore/dump for >1 GB CSV/NDJSON)
│
└── infrastructure/               # Adapters to outside systems
    ├── cache.py                  # InMemoryCache + RedisCache (CachePort protocol)
    ├── ckan_client.py            # CKAN action API client (httpx-backed). Built in
    │                             # lifespan only when AUTH_TYPE=ckan; otherwise None.
    └── engines/                  # Storage backends — one subpackage per engine
        ├── base.py               # DatastoreBackend ABC + result dataclasses
        ├── registry.py           # get_datastore_engine + get_allowed_sql_functions;
        │                         # dynamic importlib dispatch keyed on
        │                         # context.config.DATASTORE_ENGINE
        ├── bigquery/             # Engine package (one folder per backend).
        |   ├── __init__.py        # Exports `Backend = BigQueryBackend` —
        |   |                        # the registry imports `Backend`, so the
        |   |                        # concrete class name is engine-private.
        |   ├── backend.py         # DatastoreBackend subclass
        |   ├── client.py          # google-cloud-bigquery `Client` construction
        |   ├── lib.py             # Backend-specific helpers
        |   ├── metadata.py        # _table_metadata table — Frictionless schema + unique_key
        |   ├── search.py          # SQL builder for datastore_search
        |   ├── types.py           # Frictionless → BigQuery type map
        |   └── allowed_functions.txt  # Per-engine datastore_search_sql
        |                                # function allow-list — one name per
        |                                # line, `#` comments allowed.
        └── ducklake/              # Future planned engine

postman/                          # Importable Postman collection
├── collection.json               # Auto-generated from example_payload/
└── generate_postman.py           # Generator script (regenerate after edits)
```

To add a new engine (e.g. `ducklake`), drop a sibling folder following
the same layout (`__init__.py` exports `Backend = <YourBackend>`,
`backend.py` subclasses `DatastoreBackend`, plus an `allowed_functions.txt`).
`DATASTORE_ENGINE` is validated against the set of engine subdirectories
that exist at process start, and the factory imports each engine's
`Backend` via `importlib` — no `registry.py` / `config.py` edits.

## Column definitions

**Goal:** make Frictionless schema the native column shape while staying
drop-in compatible with existing CKAN clients during migration.

`datastore_create` accepts one of two input shapes:

| Shape | Keys | Status |
|---|---|---|
| Frictionless `schema` | `schema` — [Frictionless Table Schema](https://specs.frictionlessdata.io/table-schema/) | Recommended |
| Legacy CKAN `fields` | `fields`, `primary_key` | Deprecated; emits a `warnings` entry |


## Roadmap

What's shipped and what's next. Tick each box as the change set lands.

### Done

- [x] Foundation (app factory, lifespan, middleware, Dockerfile, Makefile, env config)
- [x] All six `datastore_*` actions wired end-to-end:
  - `datastore_create`, `datastore_upsert`, `datastore_delete`
  - `datastore_search` (streaming JSON / CSV / TSV; CKAN `_links` pagination)
  - `datastore_search_sql` (sqlglot parses tables + functions; per-table
    CKAN authorize; per-engine function allow-list)
  - `datastore_info` (column schema + free-form `meta` dict)
- [x] `GET /datastore/dump/<resource_id>?format=csv|ndjson|parquet` — full-table download
  via BigQuery `EXPORT DATA`. **1 shard** (≤1 GB CSV/NDJSON, or any Parquet ≤1 GB):
  302 to a GCS signed URL (server out of the byte path). **N shards** (>1 GB CSV/NDJSON):
  server stream-concats shards via async httpx (~64 KiB peak memory, no threadpool).
  Parquet >1 GB returns 413 (parquet shards can't be byte-concatenated). Results are
  cached in GCS keyed by `table.modified`; unchanged tables skip the extract entirely,
  and stale revisions are GC'd on the next cache miss so storage stays bounded to one
  rev per `(resource_id, format)`.
  `/ready` builds the rw + ro engine instances during lifespan and probes
  `engine.healthcheck()` on each — 503 with a `Service Unavailable` envelope
  if either fails (so k8s pulls the pod from the Service).
- [x] Strict request validation (Pydantic) + structured error envelopes
- [x] CKAN auth gate with TTL cache (InMemory by default; Redis when `REDIS_URL` is set)
- [x] Request context bundle (`RequestContext` / `ContextDep` / bound `CKANClient`)
- [x] Service / engine / streaming layer separation
- [x] Engine-agnostic registry — drop a folder under `infrastructure/engines/<name>/`
  exporting `Backend`; `DATASTORE_ENGINE` is validated against engine directories
  on disk, no registry / config edit required.
- [x] Real BigQuery backend (replace the placeholder in `infrastructure/engines/bigquery/backend.py`)

### Next
- [ ] Observability — JSON structured logs + request-id middleware
- [ ] Opt-in query-result cache (deferred until BigQuery lands)
- [ ] DuckLake backend (future planned engine)



## Auth

`AUTH_TYPE` selects the provider; each lives at `datastore/auth/<name>/`.

| AUTH_TYPE | What it does | Required env |
|---|---|---|
| `ckan` (default) | Calls CKAN `/api/3/action/datastore_authorize` per request. TTL-cached inside the provider so we don't hit CKAN repeatedly. | `CKAN_URL` |
| `jwt` | Verifies the bearer JWT signature + optional `aud` / `iss`. No external service. | `JWT_SECRET` (HS*) or `JWT_PUBLIC_KEY` (RS*/ES*) |
| `anonymous` | Allows every call; no identity. Local dev / CI without auth. | _(none)_ |

The orchestration in `datastore/api/auth.py` is provider-agnostic — it
owns only the boundary policy (permission whitelist, `resource_id` XOR
`package_id` rule, and the anonymous-read rule: `permission=read` calls
forward to the provider without a credential; everything else
hard-fails when the `Authorization` header is missing).

**CKAN provider.** Uses the `datastore_authorize` action, which is **not
part of stock CKAN** — it ships in the
[`ckanext-datastore-authz`](https://github.com/datopian/ckanext-datastore-authz)
extension. Before pointing this service at a CKAN instance, install
the extension and confirm the action is reachable:

```sh
curl -s "$CKAN_URL/api/3/action/datastore_authorize" \
     -H "Authorization: $CKAN_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"resource_id": "<some-resource-id>"}' | jq
```

A CKAN envelope with `success: true` and a `result.{package, resource}`
body means you're set. 404 means the extension isn't enabled in
`ckan.plugins`.

**Adding a new provider.** Drop `datastore/auth/<name>/` with an
`__init__.py` exporting `Provider = <ConcreteClass>` and a `provider.py`
implementing the `AuthProvider` Protocol (`base.py`). No registry edit
required — `AUTH_TYPE` is validated against the directories on disk at
startup, same auto-discovery as `DATASTORE_ENGINE`.

**Standalone caveat.** `datastore_create` accepts two shapes:
`resource_id` (table name only) and `resource` (a CKAN resource dict —
the service calls `ckan.resource_create(...)` first, then writes the
datastore table). The dict form is only valid under `AUTH_TYPE=ckan`;
under JWT / anonymous it's rejected with a clear validation error.



## Development setup

Requires Python 3.12+.

```sh
# Install dependencies (editable, with dev tools)
pip install -e ".[dev]"

# Run dev server
uvicorn datastore.main:app --reload



# Run tests
pytest
```

Dependencies live in `pyproject.toml` (`[project].dependencies` and `[project.optional-dependencies].dev`).

## Env vars

Every entry below maps 1:1 to a field on `datastore.core.config.Config`. See [.env.example](.env.example) for a copy-and-fill template.

| Name | Default | Purpose |
|---|---|---|
| `APP_MESSAGE` | `"Datastore API"` | Banner returned by `GET /` |
| `MAX_REQUEST_BODY_MB` | `50` | Reject request bodies larger than this (MB) |
| `DATASTORE_ENGINE` | `bigquery` | Storage backend — must match a folder under `infrastructure/engines/`; validated at startup |
| `SQL_FUNCTIONS_ALLOW_FILE` | _(empty)_ | Override path to the `datastore_search_sql` function allow-list; defaults to `<engine>/allowed_functions.txt` |
| `BIGQUERY_PROJECT` | _(empty)_ | Google Cloud project ID. Required when `DATASTORE_ENGINE=bigquery`; unset → `/ready` returns 503 with a clear warning. |
| `BIGQUERY_DATASET` | _(empty)_ | BigQuery dataset that holds per-resource tables + the engine-managed `_table_metadata`. Required when `DATASTORE_ENGINE=bigquery`; unset → metadata store is disabled and writes fall through to placeholder mode. |
| `BIGQUERY_CREDENTIALS` | _(empty)_ | Read-write service-account creds. Accepts a JSON blob (leading `{`), a path to a service-account JSON file, or empty (→ Application Default Credentials). |
| `BIGQUERY_CREDENTIALS_RO` | _(empty)_ | Read-only service-account creds (same format). Empty → falls back to `BIGQUERY_CREDENTIALS` so single-credential deployments work. |
| `BIGQUERY_USE_QUERY_CACHE` | `true` | Use BigQuery's 24h query-results cache on `datastore_search` / `datastore_search_sql` / `datastore_info`. Identical SELECTs return free + fast on cache hits. Set `false` to force a fresh scan. |
| `BIGQUERY_EXPORT_BUCKET` | _(empty)_ | GCS bucket name (no `gs://` prefix) that `/datastore/dump/<rid>` writes `EXPORT DATA` shards into. Required when the dump endpoint is in use. **Credential model: ro reads, rw writes.** RO SA (`BIGQUERY_CREDENTIALS_RO`) does the BigQuery `get_table` and the initial GCS `list_blobs` cache lookup. RW SA (`BIGQUERY_CREDENTIALS`) runs `EXPORT DATA` (it writes shards under its own identity), does GC `delete`, and signs URLs. **RO SA perms:** `bigquery.tables.get` + `storage.objects.list`. **RW SA perms:** `bigquery.jobs.create` + `bigquery.tables.export` + `bigquery.tables.getData` + `storage.objects.{create,list,delete}` + `iam.serviceAccountTokenCreator` (for V4 signing under workload identity). A 24h object-lifecycle rule on the bucket is recommended as a safety net. |
| `BIGQUERY_EXPORT_URL_EXPIRY_HOURS` | `1` | Signed-URL TTL for dump manifest entries (hours). |
| `REDIS_URL` | _(empty)_ | Redis URL for cache; empty → in-process `InMemoryCache` |
| `CKAN_URL` | _(empty)_ | Base URL of the CKAN instance (required when `AUTH_TYPE=ckan`) |
| `HTTP_TIMEOUT_SECONDS` | `10` | Timeout for outbound CKAN calls (seconds) |
| `AUTH_TYPE` | `ckan` | Auth provider — must match a folder under `datastore/auth/`. Built-in: `ckan`, `jwt`, `anonymous` |
| `AUTH_CACHE_TTL` | `10` | TTL for cached auth decisions (seconds) |
| `JWT_ALGORITHM` | `HS256` | JWT signing algorithm. HS* uses `JWT_SECRET`; RS*/ES* uses `JWT_PUBLIC_KEY` |
| `JWT_SECRET` | _(empty)_ | HS* shared secret. Required when `AUTH_TYPE=jwt` and `JWT_ALGORITHM=HS*` |
| `JWT_PUBLIC_KEY` | _(empty)_ | RS*/ES* PEM-encoded public key. Required for RS*/ES* |
| `JWT_AUDIENCE` | _(empty)_ | Expected `aud` claim. Empty = skip audience check |
| `JWT_ISSUER` | _(empty)_ | Expected `iss` claim. Empty = skip issuer check |
| `LOG_LEVEL` | `INFO` | Stdlib logging level (`DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL`) |

## API Documentation 

 http://localhost:8000/docs

## Development notes


### Adding a new endpoint

Handler in `datastore/api/endpoints/<resource>.py` (parse → call service → return CKAN envelope), request shape in `datastore/schemas/`, business logic in `datastore/services/`. Wire a new file into `datastore/api/routes.py`.


### Request context

Each endpoint takes a single `Context` that bundles the per-request
handles. The bundle wires them together so handlers stay one-liner.

```python
from datastore.api.context import Context

@router.post("/datastore_create", response_model=DatastoreCreateResponse)
async def datastore_create(
    request: Request,
    payload: DatastoreCreateRequest,
    context: Context,
):
    # Run policy + delegate to the active AuthProvider (CKAN / JWT /
    # anonymous). Pass `resource_id` (existing) or `package_id` (new) —
    # exactly one.
    data_dict = await context.authorize(
        resource_id=payload.resource_id,
        permission="create",        # read | create | update | delete | patch
    )

    # The service does the actual work (engine.create; CKAN resource_create
    # when AUTH_TYPE=ckan and the request supplies a `resource` dict).
    result = await create_datastore(context, data_dict)
    return _success_response(request, result)
```

- `context.authorize(...)` — runs the boundary policy and delegates to
  the active `AuthProvider`. Returns the `data_dict` shape
  `{"resource": <dict or {}>, "package": <dict or {}>}` ready to merge
  with the request payload.
- `context.ckan` — `CKANClient | None`, already bound to the caller's
  `api_key`. `None` under non-CKAN auth (standalone). Code paths that
  need CKAN must guard for `None`.
- `context.api_key` — the raw bearer string (parsed from the
  `Authorization` header). Provider-internal use; endpoints rarely
  touch it.
- `context.auth_provider` — the active provider instance (built once
  in the lifespan, stored on `app.state.auth_provider`).
- `context.config` — the loaded `Config`.



### Response envelopes

Every successful response follows the CKAN shape `{help, success, result}`. The base `ResponseModel` in [datastore/schemas/responses.py](datastore/schemas/responses.py) carries `help` + `success`; each endpoint subclasses it and declares an inner `Result`:

```python
class DatastoreCreateResponse(ResponseModel):
    class Result(BaseModel):
        resource_id: str
        package_id: str | None = None
        # Canonical Frictionless Table Schema (carries `primaryKey` inside).
        schema: dict[str, Any]
        # Legacy mirror — marked deprecated in OpenAPI / IDE tooltips.
        fields: Annotated[
            list[FieldSpec],
            Field(deprecated="use 'schema' (Frictionless Table Schema) instead"),
        ]
        primary_key: Annotated[
            list[str],
            Field(deprecated="use 'schema.primaryKey' instead"),
        ]
        records: list[dict[str, Any]] | None = None   # when include_records=True
        total: int | None = None                      # when include_total=True

    result: Result
```

Wire-up has three matching pieces — service return type, route `response_model`, and the runtime envelope:

```python
# service
async def create_datastore(...) -> DatastoreCreateResponse.Result: ...

# route
@router.post("/datastore_create", response_model=DatastoreCreateResponse)
async def datastore_create(...):
    return _success_response(request, await create_datastore(...))
```

`_success_response` wraps the `Result` into the full `{help, success, result}` envelope. `response_model=...` makes `/docs` document the contract; the service return type lets mypy catch drift.

Endpoints that aren't implemented yet `raise HTTPException(status_code=501, …)` — the error handler converts that to a CKAN error envelope with `__type: "Not Implemented"`.

### Adding a new env var

1. Add a `Field(default=..., description=...)` to `Config` in [datastore/core/config.py](datastore/core/config.py) (with bounds where appropriate: `ge=`, `le=`, `Literal[...]`).
2. Mirror the var in `.env.example` with a safe default and a one-line comment.
3. Document it in the "Env vars" table above.

### Raising errors

Endpoints (and services they call) should raise from `datastore/core/exceptions.py` — never return error envelopes by hand:

```python
from datastore.core.exceptions import NotFoundError, AuthorizationError, ValidationError

raise NotFoundError(f"resource '{rid}' not found")
```

`datastore/api/error_handlers.py` converts each `APIError` subclass to the matching CKAN envelope + status code.

### Testing

Tests live in [tests/](tests/), organised by what they exercise:

```
tests/
├── conftest.py                   # FakeCKAN + InMemoryCache + TestClient fixture
├── test_health.py                # /, /health, /ready
├── test_datastore_*.py           # End-to-end per endpoint (TestClient)
├── test_read_service.py          # Direct service calls — no HTTP
├── test_write_service.py
│
├── auth/                         # Auth layer — one folder per provider
│   ├── test_base.py              # Decision + default_key_id
│   ├── test_registry.py          # AUTH_TYPE dispatch
│   ├── test_orchestration.py     # api/auth.py boundary policy
│   ├── ckan/test_provider.py     # CKAN provider + TTL cache
│   ├── jwt/test_provider.py      # JWT signature / aud / iss / exp
│   └── anonymous/test_provider.py
│
└── engines/
    ├── bigquery/test_*.py        # Real BigQuery backend, fully mocked
    └── ducklake/                 # (placeholder for future engine)
```

The `client` fixture in `conftest.py` wires up `FakeCKAN` (in-memory
CKAN stand-in) and an `InMemoryCache` via `app.dependency_overrides`,
and installs a `CKANAuthProvider` backed by the fake. No real network
calls. `FakeCKAN` exposes `add_resource(...)`, `add_package(...)`,
`deny(api_key)` and an `authorize_calls` counter to assert cache
behaviour.

