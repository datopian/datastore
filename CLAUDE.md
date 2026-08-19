# Datastore API Service
A CKAN-compatible datastore API. Tabular data CRUD + search over a pluggable
storage backend (BigQuery Datastore or Ducklake as future support). 

---

## 1. Goals

- CKAN-compatible request/response shapes for `/datastore/api/v2/datastore_*`.
- **Pluggable storage backend** selected by `DATASTORE_ENGINE` (`bigquery` today; `ducklake` planned).
- **Pluggable auth** selected by `AUTH_TYPE` (`ckan` / `jwt` / `anonymous`). Provider lives in `datastore/auth/<name>/`; only the CKAN provider touches the network, and its TTL cache is local to that provider.
- **Standalone-capable** — runs without an upstream CKAN under `AUTH_TYPE=anonymous` or `AUTH_TYPE=jwt`. CKAN is only required when `AUTH_TYPE=ckan`.
- **Streaming search responses** (peak memory ≈ 1 row) for `datastore_search` / `datastore_search_sql`; the sharded-parquet download streams a zip at ≈ 1 chunk.
- Strict request validation, structured CKAN-shaped error envelopes.


## 2. Technology Stack

| Concern | Choice | Why |
|---|---|---|
| Web framework | **FastAPI** (`fastapi[standard]`) | Async, OpenAPI for free, dependency injection |
| ASGI server | **uvicorn** + `uvloop` + `httptools` | Fast async I/O |
| Validation | **Pydantic v2** (request only) | Strict shape validation, no per-row cost |
| JSON | **orjson** | 5–10× stdlib `json`, returns bytes, datetime-aware |
| Datastore backend | **google-cloud-bigquery** | Managed, cached, scalable |
| HTTP client | **httpx** (`AsyncClient`) | Connection-pooled CKAN calls |
| Cache | **redis** + `hiredis` | TTL cache for CKAN auth decisions |
| Schema validation | **frictionless** | Field schema validation on `datastore_create` |
| SQL parsing | **sqlglot** | Parse `datastore_search_sql` — pull table + function names for the auth + allow-list gates |
| JWT auth | **PyJWT** | HS*/RS*/ES* signature + `aud`/`iss`/`exp` validation for the JWT provider |


`pyproject.toml` dependencies (live):
```toml
[project]
dependencies = [
    "fastapi[standard]>=0.115,<0.116",
    "pydantic>=2.7,<3",
    "pydantic-settings>=2.3",
    "orjson>=3.10",
    "google-cloud-bigquery>=3.25",
    "redis[hiredis]>=5.0",
    "httpx>=0.27",
    "frictionless>=5.18",
    "uvloop>=0.21",
    "httptools>=0.6",
    "sqlglot>=25.0",
    "pyjwt>=2.8,<3",
]

[tool.ruff.lint]
select = ["E", "F", "I"]

[tool.mypy]
strict = true
```

---

## 3. Folder Structure

**Stack split.** Two libraries do most of the heavy lifting and each has one
home in the tree:

- **Starlette** — the web part. Lives in `datastore/api/` and `datastore/main.py`. Everything that
  touches `Request`, `Response`, `StreamingResponse`, middleware, status codes,
  routing, or `Depends` lives here. Nothing else imports from `fastapi` or
  `starlette`.
- **Pydantic** — the data part. Lives in `datastore/schemas/` (request/response
  models) and `datastore/core/config.py` (`BaseSettings`). Used for **boundary
  validation only** — never as the internal data type passed between services
  or returned from engines (those use plain dicts, dataclasses, and tuples to
  keep per-row cost at zero).

### Layer rule

```
api  ──▶  services  ──▶  infrastructure
 │           │                 ▲
 ├──▶  auth  ─────────────────┤
 │                            │
 └──▶  schemas ◀──────────────┘     (schemas = Pydantic models, plain data)
```

One-way dependencies. `infrastructure/` never imports from `api/`,
`services/`, or `auth/`. `services/` and `auth/` never import from
`api/`. `api/` is the only layer that knows about FastAPI/Starlette.
`auth/` may use `infrastructure/` adapters (the CKAN provider needs
`CKANClient`, all providers may use `CachePort`).

### Tree

```
datastore-api/
│
├── pyproject.toml                    # Project metadata + deps + tool config
├── README.md
├── CLAUDE.md                         # This document — design + execution plan
├── .env.example                      # Template for env vars (every Config field)
├── .gitignore
├── Makefile                          # run, test, lint, format
├── docker-compose.yml                # local: app + redis + ckan
├── Dockerfile
│
├── datastore/
│   ├── __init__.py
│   ├── main.py                       # FastAPI app factory: create_app() +
│   │                                 # lifespan (httpx client, cache, ckan client);
│   │                                 # registers middleware + exception handlers;
│   │                                 # module-level `app = create_app()` for uvicorn.
│   │
│   │ ── 1. API LAYER ─────────────────────────  (FastAPI + Starlette live here)
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py                 # Top-level APIRouter; mounts endpoints/
│   │   ├── context.py                # RequestContext + ContextDep — per-request
│   │   │                             # handles (config, api_key, auth_provider, ckan)
│   │   │                             # with an `.authorize()` method that delegates
│   │   │                             # to auth.py
│   │   ├── auth.py                   # Provider-agnostic boundary policy: permission
│   │   │                             # whitelist, resource_id XOR package_id rule,
│   │   │                             # anonymous-read rule. Delegates to the active
│   │   │                             # AuthProvider; no caching here (CKAN caches
│   │   │                             # internally — see datastore/auth/ckan/).
│   │   ├── responses.py              # CKAN envelope helpers (_success_response / _error_response)
│   │   │                             # + orjson-backed ORJSONResponse
│   │   ├── error_handlers.py         # APIError / HTTPException / RequestValidationError
│   │   │                             # → CKAN error envelope mapping
│   │   ├── middleware.py             # ASGI middleware (BodySizeLimitMiddleware today)
│   │   ├── static/                   # Vendored Swagger UI + docs theme (no CDN)
│   │   │   ├── swagger-ui/           #   swagger-ui-dist 5.17.14 (css + bundle)
│   │   │   └── theme/theme.css       #   theme ported from ckanext-openapidocs
│   │   ├── templates/docs.html       # Jinja template for the Swagger UI page
│   │   └── endpoints/                # One module per resource group
│   │       ├── __init__.py
│   │       ├── health.py             # /, /health, /ready (CKAN-shaped envelopes)
│   │       └── datastore.py          # /datastore/api/v2/datastore_*
│   │
│   │ ── 2. AUTH PROVIDERS ───────────────────────  (one subpackage per AUTH_TYPE)
│   ├── auth/
│   │   ├── __init__.py
│   │   ├── base.py                   # AuthProvider Protocol + Decision dataclass +
│   │   │                             # default_key_id (JWT jti / sha256 cache-key helper)
│   │   ├── registry.py               # get_auth_provider(config, **extras) —
│   │   │                             # importlib dispatch by AUTH_TYPE
│   │   ├── ckan/                     # AUTH_TYPE=ckan
│   │   │   ├── __init__.py           #   exports `Provider = CKANAuthProvider`
│   │   │   └── provider.py           #   datastore_authorize via CKANClient + TTL cache
│   │   ├── jwt/                      # AUTH_TYPE=jwt
│   │   │   ├── __init__.py           #   exports `Provider = JWTAuthProvider`
│   │   │   └── provider.py           #   PyJWT verify (HS*/RS*/ES* + aud/iss/exp)
│   │   └── anonymous/                # AUTH_TYPE=anonymous
│   │       ├── __init__.py           #   exports `Provider = AnonymousAuthProvider`
│   │       └── provider.py           #   always allows; no identity
│   │
│   │ ── 3. CORE (cross-cutting, framework-agnostic) ──────
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py                 # Pydantic-Settings `Config` (env-driven) +
│   │   │                             # `get_config()` lru-cached factory
│   │   ├── constants.py              # Shared constants (POSTGRES_TYPES map)
│   │   ├── exceptions.py             # APIError taxonomy: ValidationError,
│   │   │                             # AuthorizationError, NotFoundError,
│   │   │                             # ConflictError, ServerError +
│   │   │                             # HTTP_STATUS_TO_TYPE_LABEL map
│   │   └── helper.py                 # Pure helpers (parse_authorization_header, …)
│   │
│   │ ── 4. SCHEMAS (Pydantic — boundary validation only) ──
│   ├── schemas/                      # Inbound request bodies + outbound response
│   │   ├── __init__.py               # types. Never passed between services or
│   │   ├── request.py                # returned from engines.
│   │   │                             #   request.py    – DatastoreCreateRequest,
│   │   │                             #                   DatastoreUpsertRequest,
│   │   │                             #                   DatastoreSearchRequest
│   │   ├── responses.py              #   responses.py  – ResponseModel base +
│   │   │                             #                   per-endpoint envelopes
│   │   │                             #                   (StatusResponse,
│   │   │                             #                   DatastoreCreateResponse)
│   │   └── validators.py             #   validators.py – FieldSpec, StringOrList,
│   │                                 #                   PostgresType, helper fns
│   │
│   │ ── 5. SERVICES (business logic, plain Python) ──────
│   ├── services/                     # Orchestration: validate → call engine →
│   │   ├── __init__.py               # shape result. Inputs: plain types or
│   │   ├── write.py                  # validated schemas. Outputs: typed response
│   │   │                             # models. No FastAPI, no raw SQL.
│   │   │                             #   write.py     – create / upsert / delete
│   │   ├── read.py                   #   read.py      – search / search_sql / info
│   │   │                             #                  (engine call, format
│   │   │                             #                  dispatch, pagination links,
│   │   │                             #                  function allow-list)
│   │   └── streaming.py              #   streaming.py – byte-yielding writers
│   │                                 #                  (objects/lists/csv/tsv
│   │                                 #                   + zip_archive_writer)
│   │
│   │ ── 6. INFRASTRUCTURE (adapters to the outside world) ─
│   └── infrastructure/
│       ├── __init__.py
│       ├── cache.py                  # CachePort (Protocol) + InMemoryCache +
│       │                             # RedisCache (TTL-based)
│       ├── ckan_client.py            # CKANClient — httpx async wrapper around
│       │                             # CKAN /api/3/action; bind(api_key) per request
│       └── engines/                  # One subpackage per backend.
│           ├── __init__.py           # Re-exports get_datastore_engine, Mode
│           ├── base.py               # DatastoreBackend ABC +
│           │                         # SearchResult / WriteResult dataclasses
│           ├── registry.py           # get_datastore_engine + get_allowed_sql_functions;
│           │                         # dynamic `importlib` dispatch keyed on
│           │                         # context.config.DATASTORE_ENGINE
│           ├── bigquery/            # Engine package (one folder per backend).
│           |   ├── __init__.py       # Exports `Backend = BigQueryBackend` —
│           |   |                       # registry imports `Backend`, so the
│           |   |                       # concrete class name is engine-private.
│           |   ├── backend.py        # DatastoreBackend subclass (placeholder)
│           |   ├── export.py         # Download pipeline (dump/dump_sql):
│           |   |                       # cache → EXPORT DATA → compose → sign
│           |   ├── client.py         # google-cloud-bigquery `Client` construction
│           |   ├── lib.py            # Backend-specific helpers (optional)
│           |   └── allowed_functions.txt  # Per-engine datastore_search_sql
│           |                               # function allow-list — one name per
│           |                               # line, `#` comments allowed.
│           └── ducklake/             # Future planned engine
└── tests/
    ├── __init__.py
    ├── conftest.py                   # FakeCKAN, InMemoryCache, TestClient fixture;
    │                                 # autouse _isolate_bigquery_env clears BQ envs;
    │                                 # CKAN pytest plugin disabled via pyproject
    ├── test_health.py
    ├── test_datastore_*.py           # End-to-end per endpoint (TestClient)
    ├── test_read_service.py          # Direct service calls — no HTTP
    ├── test_write_service.py
    ├── auth/                         # One folder per auth provider, mirrors datastore/auth/
    │   ├── test_base.py              # Decision + default_key_id
    │   ├── test_registry.py          # AUTH_TYPE dispatch
    │   ├── test_orchestration.py     # api/auth.py boundary policy
    │   ├── ckan/test_provider.py     # CKAN provider + TTL cache
    │   ├── jwt/test_provider.py
    │   └── anonymous/test_provider.py
    └── engines/
        ├── bigquery/test_*.py        # Real BigQuery backend, fully mocked
        └── ducklake/                 # (placeholder for future engine)
```

**Adding a new engine** — drop a sibling folder with the same layout
(`__init__.py` exports `Backend = <YourBackend>`; `backend.py` is the
`DatastoreBackend` subclass; `client.py` / `lib.py` for backend-specific
construction + helpers, both optional; `allowed_functions.txt` lists
allowed SQL functions). No edit to `registry.py` or `config.py` is
required — `DATASTORE_ENGINE` validates against the set of engine
subdirectories that exist at startup, and the factory dispatches via
`importlib.import_module` keyed off the `Backend` alias. The `ducklake`
adapter will live at `infrastructure/engines/ducklake/` when it lands.

`scripts/` and `docs/` are intentionally absent today. Add them when there's a concrete need
(seed scripts, operational runbooks). Until then the README + this file are the docs.

### What goes where — rules of thumb

| Folder | Put here | Do NOT put here |
|---|---|---|
| `datastore/main.py` | App factory, lifespan (httpx, cache, auth provider, engines), middleware order, handler registration | Routes, business logic |
| `datastore/api/endpoints/` | Route declarations, request parsing, response building | SQL, engine calls, validation rules — delegate to services |
| `datastore/api/context.py` | `RequestContext`, `ContextDep`, `get_context`, `get_auth_provider`, `get_ckan_client` (per-request DI bundle) | The logic those handles invoke — that lives in `services/` / `auth/` / `infrastructure/` |
| `datastore/api/auth.py` | Provider-agnostic boundary policy (permission whitelist, anonymous-read rule, resource_id XOR package_id) | Concrete provider behaviour — CKAN/JWT/anonymous logic lives in `datastore/auth/<name>/` |
| `datastore/api/responses.py` | Envelope helpers, `ORJSONResponse`. `_help` deep-links into Swagger via `api/docs.py`'s `help_url` | Anything that needs DB access |
| `datastore/api/error_handlers.py` | Exception → CKAN error envelope mapping | Business rules — raise `APIError` from wherever the rule lives |
| `datastore/api/static/` | Vendored front-end assets served at `/datastore/api/static` — Swagger UI dist + `theme/theme.css` | Anything generated at runtime; anything Python imports |
| `datastore/api/templates/` | Jinja templates for HTML pages (`docs.html`) | Anything returning JSON — those go through `api/responses.py` |
| `datastore/auth/<name>/` | Concrete `AuthProvider` implementation: `__init__.py` exports `Provider = <ConcreteClass>`; `provider.py` implements `authorize` + `key_id`. CKAN provider holds its own TTL cache. | Cross-provider policy (that's `api/auth.py`); FastAPI imports |
| `datastore/auth/base.py` | `AuthProvider` Protocol, `Decision` dataclass, `default_key_id` helper | Provider implementations |
| `datastore/auth/registry.py` | importlib factory keyed on `AUTH_TYPE` | Instance caching — the lifespan builds once and stashes on `app.state` |
| `datastore/core/` | Config (`Config`), exceptions, constants, pure helpers | I/O, FastAPI imports, business orchestration |
| `datastore/schemas/` | Pydantic `BaseModel` request / response / validator types | Methods that do work — schemas are data shapes only |
| `datastore/services/` | Validation that needs cross-input context, calls to engines/cache/CKAN, result shaping | `fastapi`/`starlette` imports, raw SQL strings, HTTP clients (call adapters) |
| `datastore/infrastructure/` | Adapters: cache (Redis / in-memory), CKAN HTTP client, storage engines (BigQuery / DuckLake) | Business rules, FastAPI types, orchestration, auth providers (those are at `datastore/auth/`) |
| `tests/` | Test code only — `tests/auth/<name>/` mirrors `datastore/auth/<name>/`; `tests/engines/<name>/` mirrors `datastore/infrastructure/engines/<name>/` | Fixtures that reach into production internals through back doors — go through the public API |

### Hard rules

1. **Only `datastore/api/` and `datastore/main.py` may import from `fastapi` or `starlette`.**
   Greppable invariant: `rg "from (fastapi|starlette)" datastore/services datastore/infrastructure datastore/core datastore/auth` must return nothing.
2. **Only `datastore/schemas/` and `datastore/core/config.py` may import from `pydantic` / `pydantic_settings`.**
   Engines, services, and auth providers pass plain dicts, tuples, and dataclasses.
3. **Engines return a lazy row iterator of tuples, never `list[dict]`.** Streaming
   peak memory ≈ 1 row regardless of result size.
4. **Pydantic validates at the boundary; orjson serialises out.** Don't use
   `model.model_dump()` on hot paths — build dicts inline and `orjson.dumps()`.
5. **Auth providers and storage engines are plugins, not registries to edit.** Drop a folder under `datastore/auth/<name>/` or `datastore/infrastructure/engines/<name>/` with `__init__.py` exporting `Provider` / `Backend`; `AUTH_TYPE` / `DATASTORE_ENGINE` are auto-validated against directories on disk. No `registry.py` or `config.py` edit required to add either.
6. **Auth caching is provider-private.** The only "auth cache" in the codebase is the TTL cache inside `datastore/auth/ckan/provider.py` (network round trip; worth caching). JWT and anonymous are local and never cache.
7. **No `container.py` / DI framework.** FastAPI's `Depends` plus the two `registry.py` factories (auth + engines) are the only wiring mechanisms.

---

## 4. Architecture

```mermaid
flowchart TB
    Client([Client])

    subgraph K8S["Kubernetes cluster"]
        direction TB
        Ingress["Ingress<br/>TLS, host routing"]
        Service["Service<br/>ClusterIP"]
        HPA["HorizontalPodAutoscaler<br/>CPU + req rate"]

        subgraph Deploy["Deployment (N replicas)"]
            direction LR
            Pod1["Pod<br/>FastAPI + uvicorn"]
            Pod2["Pod<br/>FastAPI + uvicorn"]
            PodN["Pod<br/>..."]
        end

        Config["ConfigMap<br/>DATASTORE_ENGINE<br/>BQ_PROJECT<br/>MAX_REQUEST_BODY_MB<br/>AUTH_CACHE_TTL"]
        Secret["Secret<br/>CKAN API key<br/>BQ_CREDENTIALS_JSON<br/>REDIS_URL"]
        Redis[("Redis<br/>StatefulSet or managed<br/>auth + query cache")]
    end

    CKAN["CKAN<br/>/api/3/action/datastore_authorize<br/>(only when AUTH_TYPE=ckan)"]
    BQ["BigQuery API<br/>datastore backend"]

    Client -->|HTTPS| Ingress
    Ingress --> Service
    Service --> Pod1
    Service --> Pod2
    Service --> PodN
    HPA -.scales.-> Deploy

    Pod1 -.reads.-> Config
    Pod1 -.reads.-> Secret
    Pod1 -->|auth cache (CKAN provider only)| Redis
    Pod1 -.->|on cache miss| CKAN
    Pod1 -->|queries| BQ

    classDef ext fill:#fff5e6,stroke:#d97706,color:#7c2d12
    classDef k8s fill:#eef6ff,stroke:#2563eb,color:#1e3a8a
    classDef store fill:#ecfdf5,stroke:#059669,color:#064e3b
    class CKAN,BQ ext
    class Ingress,Service,HPA,Pod1,Pod2,PodN,Config,Secret k8s
    class Redis store
```

Inside each pod:

```mermaid
flowchart LR
    HTTP([HTTP request]) --> Uvicorn["uvicorn"]
    Uvicorn --> MW["api/middleware.py\nbody-size + GZip"]
    MW --> Ctx["api/context.py\nget_context → RequestContext"]
    Ctx --> Routes["api/endpoints/\ndatastore.py + health.py"]
    Routes --> Auth["api/auth.py\nboundary policy"]
    Auth --> Provider["auth/<AUTH_TYPE>/provider.py\n(ckan / jwt / anonymous)"]
    Provider -->|CKAN provider only| Cache[("infrastructure/cache.py\nInMemory or Redis")]
    Provider -.->|CKAN provider, on miss| CKANSvc["CKAN\n/api/3/action/datastore_authorize"]
    Routes --> Svc["services/\nwrite.py + read.py + streaming.py"]
    Svc --> Eng["infrastructure/engines/\nregistry.get_datastore_engine"]
    Eng --> BQ["bigquery/backend.py"]
    Eng --> DL[("ducklake/ (planned)")]
    Routes --> Resp["api/responses.py\n_success_response / _error_response"]
    Resp --> Schema["schemas/responses.py\nResponseModel + Result"]

    classDef ext fill:#fff5e6,stroke:#d97706,color:#7c2d12
    classDef store fill:#ecfdf5,stroke:#059669,color:#064e3b
    class CKANSvc,BQ ext
    class Cache,DL store
```

**Layer responsibilities**

| Layer | Lives in | Knows about |
|---|---|---|
| HTTP | `api/endpoints/`, `api/routes.py`, `api/middleware.py` | Request parsing, status codes, FastAPI |
| Request bundle | `api/context.py` | Per-request handles: config, api_key, auth_provider, ckan (Optional). `.authorize()` method delegates to `api/auth.py` |
| Auth boundary policy | `api/auth.py` | Permission whitelist, anonymous-read rule, validation — provider-agnostic |
| Auth providers | `auth/<name>/` | One per `AUTH_TYPE`. CKAN (network + TTL cache), JWT (PyJWT verify), anonymous (no-op) |
| Response | `api/responses.py`, `schemas/responses.py` | CKAN envelope shape, orjson, typed result models |
| Errors | `api/error_handlers.py`, `core/exceptions.py` | APIError taxonomy → status code + `__type` label |
| Business logic | `services/` | Orchestration — no FastAPI, no raw SQL, no HTTP plumbing |
| Storage | `infrastructure/engines/` | Backend ABC + concrete adapters; SQL dialect, connection management, row iterators |
| External adapters | `infrastructure/cache.py`, `infrastructure/ckan_client.py` | TTL cache (InMemory / Redis), httpx-based CKAN client |
| Cross-cutting | `core/` | Config, constants, exceptions, pure helpers |

**Key design rules**
- Endpoints call `context.authorize(...)` then services; services call engines. Endpoints never touch SQL.
- `services/write.py` `datastore_create` is the only path that uses `context.ckan` (for `resource_create` on the dict-resource branch); the endpoint gates that branch on `AUTH_TYPE=ckan`. All other endpoints work standalone.
- Engines return `SearchResult` with a **lazy row iterator of tuples** — never `list[dict]`. Peak memory ≈ 1 row regardless of result size.
- Pydantic validates inbound (`schemas/request.py`) and documents outbound (`schemas/responses.py`). Outbound serialisation goes through `_success_response` → `ORJSONResponse` → orjson.
- The CKAN client is built once in the lifespan **only when `AUTH_TYPE=ckan`**; `get_context` binds the caller's `api_key` per request (a shallow `.bind(api_key)` copy). Under non-CKAN auth `app.state.ckan` is `None` and the per-request bound client is `None`.
- The auth provider is built once in the lifespan (with cache + cache_ttl + ckan client passed as kwargs); registry returns a fresh instance on every call so the lifespan owns instance reuse.
- No DI container. FastAPI's `Depends` + the two `registry.py` factories (auth + engines) are the only wiring mechanisms.

**Pod-level shape**
- One container per pod: the FastAPI app. Sidecars only for observability (e.g., OpenTelemetry collector).
- `livenessProbe` → `GET /datastore/api/health` (always 200 while the process is up).
- `readinessProbe` → `GET /datastore/api/ready` (200 only when both backends pass `healthcheck()`; pod pulled from Service when 503).
- `terminationGracePeriodSeconds: 30` so in-flight streaming responses drain before SIGKILL.
- Memory bounded by `MAX_REQUEST_BODY_MB` × concurrency for writes; search responses are O(1) peak memory.

**Cluster-level shape**
- `Deployment` with N replicas, fronted by a `ClusterIP` `Service`.
- `Ingress` (NGINX, Traefik, etc.) terminates TLS and routes by host/path.
- `HorizontalPodAutoscaler` on CPU + custom metric (request rate).
- Config: non-secret env vars in `ConfigMap` (`DATASTORE_ENGINE`, `MAX_REQUEST_BODY_MB`, `BQ_PROJECT`, `AUTH_CACHE_TTL`, `HTTP_TIMEOUT_SECONDS`); secrets in `Secret` (CKAN API key, `BQ_CREDENTIALS_JSON`, `REDIS_URL`).
- Redis as in-cluster `StatefulSet` or external managed instance — connection string from Secret. Empty `REDIS_URL` falls back to the in-process `InMemoryCache` (single-pod only).
- DuckLake backend will require single-replica `StatefulSet` + `PersistentVolumeClaim` (when implemented); BigQuery backend supports horizontal `Deployment`.

---

## 5. API Surface

All datastore endpoints sit under `/datastore/api/v2/` to match the CKAN action API.
Health endpoints at the root.

### 5.1 Health

All three return the CKAN envelope shape `{help, success, result: {...}}`.

| Method | Path | Status | Result |
|---|---|---|---|
| GET | `/datastore/api/health` | implemented | `{"status": "ok"}` — liveness; always 200 if process is up |
| GET | `/datastore/api/ready` | implemented | `{"status": "ready"}` — calls `engine.healthcheck()` for rw + ro; 503 with a `Service Unavailable` envelope if either fails |

### 5.2 Datastore endpoints

Each endpoint takes a single `ContextDep`. The handler calls `context.authorize(...)` (which runs the boundary policy + delegates to the active `AuthProvider`) and then delegates to a service in `services/`.

| Method | Path | Status | Body / Params | Response model |
|---|---|---|---|---|
| POST | `/datastore/api/v2/datastore_create` | **implemented** | `DatastoreCreateRequest` | `DatastoreCreateResponse` |
| POST | `/datastore/api/v2/datastore_upsert` | **implemented** | `DatastoreUpsertRequest` | `DatastoreUpsertResponse` |
| POST | `/datastore/api/v2/datastore_delete` | **implemented** | `DatastoreDeleteRequest` | `DatastoreDeleteResponse` |
| GET  | `/datastore/api/v2/datastore_search` | **implemented** (streaming) | `DatastoreSearchRequest` | `DatastoreSearchResponse` |
| GET  | `/datastore/api/v2/datastore_search_sql` | **implemented** (streaming) | `DatastoreSearchSQLRequest` | `DatastoreSearchResponse` |
| GET  | `/datastore/api/dump/query` | **implemented** | `sql=<SELECT…>`, `format=csv\|gzip\|ndjson\|parquet` | 302 → GCS *or* streaming body (see §5.3) |
| GET  | `/datastore/api/v2/datastore_info` | **implemented** | `DatastoreInfoRequest` | `DatastoreInfoResponse` |
| GET  | `/datastore/api/dump/{resource_id}` | **implemented** | `format=csv\|ndjson\|parquet` | 302 → GCS *or* streaming body (see §5.3) |

The BigQuery engine is wired end-to-end: DDL, MERGE-based upsert, DML delete, parameterised search, native table-level metadata (the Frictionless schema + unique_key are JSON-encoded into the table's own `description` OPTION) for the schema round-trip, a row-count fast path via `INFORMATION_SCHEMA.TABLE_STORAGE`, and `EXPORT DATA`-backed dump with `table.modified`-keyed GCS caching. The DuckLake engine is the next concrete adapter — see §7.

`datastore_create` accepts two shapes:

- `resource_id` — table name only. Works under any `AUTH_TYPE`.
- `resource` (dict) — calls `ckan.resource_create(...)` first to materialise a CKAN resource, then writes the datastore table. The resource is created with `url_type="datastore"` so CKAN (and the read-only guard below) knows the datastore owns its data. **Only valid under `AUTH_TYPE=ckan`**; the endpoint rejects this shape with a `Validation Error` under JWT / anonymous since there's no CKAN to land it.

**Read-only guard (`AUTH_TYPE=ckan` only).** `datastore_create`, `datastore_upsert`, and `datastore_delete` refuse to write a resource whose CKAN record carries `url_type="datastore"` unless the request sets `force: true` — a `Validation Error` ("Cannot update a read-only resource. Use \"force\" to force update.") otherwise. This mirrors CKAN's protection against clobbering datastore-managed data by accident. The guard is gated on `AUTH_TYPE=ckan` and skipped entirely under any other provider (only the CKAN provider attaches a resource record).

### 5.3 `GET /datastore/api/dump/{resource_id}`

Full-table download, **one URL → one file** from the caller's point of
view. Bytes never pass through API memory — the one exception is a
sharded parquet export, which is zipped on the way out.

Pipeline (`_prepare_download` in [bigquery/export.py](datastore/infrastructure/engines/bigquery/export.py)):

1. **Resolve cache key** — read `table.modified`, `rev = hex(microsec_epoch(modified))`, prefix `dumps/<rid>/<fmt>/<rev>/`. Everything this service writes lives under the single `dumps/` root — table dumps keyed on the resource id, query dumps on a SQL hash — so one lifecycle rule covers it all. Every request for that (resource, format, table version) shares the **revision directory**; each individual export writes into its own `<attempt-uuid>/` beneath it:

```
dumps/<rid>/<fmt>/<rev>/          ← cache key, shared by all requests
        └── <attempt-uuid>/       ← one export's private scratch
                ├── part_*.<ext>
                ├── data.<ext>    ← composed (csv/gzip, ndjson >1 shard)
                └── _SUCCESS      ← written last; publishes the attempt
```
2. **Cache lookup** (`_complete_attempt`) — one `list_blobs(prefix=…)`, grouped by attempt directory; serve the newest attempt carrying `_SUCCESS`. Attempts without it are in-flight or abandoned: never served, never deleted on the response path. Within the winner a composed `data.<ext>` is the whole download; `part_*` shards beside it are leftovers awaiting background deletion. Parquet never composes, so its shards *are* the download.
3. **Submit `EXPORT DATA`** — wildcard URI `gs://<bucket>/<prefix><attempt>/part_*.<ext>`. The wildcard is mandatory (BigQuery rejects a bare URI — *"Option 'uri' value must be a wild card URI"*) and shards by write parallelism, not size, so a 40 MB result routinely lands as several files. `job.result()` waits, holding one worker thread for the export's duration. CSV **and gzip** export **header-less** (gzip adds `compression='GZIP'`, so BigQuery does the compressing); the SELECT casts `TIMESTAMP` + `DATETIME` to ISO 8601 for CSV/NDJSON and `TO_JSON_STRING` for JSON columns on Parquet.
4. **Compose** — csv/gzip/ndjson shards are stitched into ONE object server-side with GCS `compose` (≤32 sources per call, chained beyond that). csv and gzip compose a synthesized header member in first — plain bytes for csv, a gzip member for gzip — so the result carries exactly one header. (Verified against the real bucket: BigQuery's gzip objects carry **no** `Content-Encoding`, so compose byte-concats them and multi-member gzip decompresses as one file.) Parquet is never composable (footer + magic bytes) and stays as shards. Compose sources are the explicit shard list from **this attempt only** — never a fresh listing — so nothing foreign can be swept into the output.
5. **Publish** — write the zero-byte `<prefix><attempt>/_SUCCESS`. This is the commit point: the attempt is unreadable before it, readable after, so no caller ever sees a half-written export. Written *after* the compose, so it can never become file content.
6. **Cleanup — in the background** (`_cleanup_in_background`; never on the response path): the compose source shards are deleted, and superseded attempts + revisions under `dumps/<rid>/<fmt>/` are swept (`_delete_old_cache` — this request's own attempt is never touched, and the sweep is **always age-gated by the signed-URL expiry** so a sibling attempt that may still be exporting, or whose URLs are live, survives).
7. **Sign URLs** — V4 with `response-content-disposition: attachment; filename="<rid>.<ext>"` (one file) or `<rid>_NN.<ext>` (multi-file parquet, 1-indexed). Signing is offloaded to a thread (IAM round-trip under workload identity).
8. **Return** (`download_response` in [api/endpoints/dump.py](datastore/api/endpoints/dump.py)):
   - 1 URL → `RedirectResponse(302)`. Bytes flow GCS → client; the server is **out of the byte path** for every format, and downloads are resumable.
   - N URLs (sharded parquet only) → `200` + **a streamed zip** of the parts (`zip_archive_writer` in [services/streaming.py](datastore/services/streaming.py)). The API fetches each signed URL over `app.state.http` and frames it into the archive a chunk at a time, so one export is always one file at one URL. Entries are `ZIP_STORED` (parquet is already compressed; deflating costs CPU for no size win) and `force_zip64=True` (member sizes aren't known up front). This is the **only** path where the server carries the bytes — no `Content-Length`, no range support, and a fetch failure mid-archive truncates a response that already returned 200.

Errors:
- Any BigQuery / GCS failure → `ServerError` (500) with the upstream message. A ">1 GB single URI" failure is classified by `_is_export_too_large` into `PayloadTooLargeError` (413) — defensive only, since the wildcard URI means BigQuery shards instead of failing.
- `BIGQUERY_EXPORT_BUCKET` unset → `ServerError` at request time (the lifespan doesn't fail-fast because dump is an optional capability).
- **Concurrency.** Requests for the same (query, table version) share a revision directory but export into **separate attempt directories**, so they can never overwrite each other's objects, and neither is servable until its own `_SUCCESS` lands. The residual cost is that both run an export — duplicate billed scans, bounded and rare. Fixing that needs single-flighting (an in-process lock, or a deterministic BigQuery job id so the loser waits on the winner's job); it is a cost optimisation, not a correctness one, and is deliberately not implemented.

Required IAM. Dump follows a strict **ro for reading, rw for writing/updating** model — see [bigquery/client.py](datastore/infrastructure/engines/bigquery/client.py) `load_credentials` + `_build_bq_client` / `_build_storage_client` on the backend:

| Step | Identity | Why |
|---|---|---|
| `get_table` | RO BQ (`self.client`) | Reading BigQuery metadata. |
| `list_blobs` cache lookup | RO GCS | Reading GCS objects. |
| `client.query("EXPORT DATA …")` | RW BQ (built on demand) | BigQuery writes shards to GCS under this SA's identity — it's a write op even though the SQL surface is `SELECT`. |
| Post-extract `list_blobs` refresh | RW GCS | Blobs are passed straight to `generate_signed_url` next; we want them bound to the rw client. |
| `compose` (csv/ndjson) | RW GCS | Server-side concat into one object: reads each source's metadata (`storage.objects.get` — **`list` alone is not enough**) and creates the composite. |
| `upload_from_string` (csv header member) | RW GCS | The one-row header composed in front of the header-less csv shards. |
| `delete` (GC) | RW GCS | Writing/deleting objects. |
| `generate_signed_url` | RW GCS | Under workload identity this calls IAM `signBlob`, which typically only the rw SA holds via `iam.serviceAccountTokenCreator`. |

Concrete perm sets:

- **RO SA** (`BIGQUERY_CREDENTIALS_RO`) — `bigquery.tables.get` + `storage.objects.list`.
- **RW SA** (`BIGQUERY_CREDENTIALS`) — `bigquery.jobs.create` + `bigquery.tables.export` + `bigquery.tables.getData` + `storage.objects.{create,get,list,delete}` + `iam.serviceAccountTokenCreator`. **`get` is required by `compose`** (it reads each source object); a role with only create/list/delete 403s on the compose step.

A single SA works if both perm sets land on the same identity — `BIGQUERY_CREDENTIALS_RO` empty falls through to ADC; same env var can drive both. `_build_bq_client` and `_build_storage_client` on the backend are deliberately small + stub-friendly so tests inject mocks without monkey-patching `google.cloud.*` globally.

A 24h object-lifecycle rule on the bucket is **required** in practice: the engine GCs older revs already, but lifecycle is the only thing that cleans abandoned `dumps/<qhash>/` prefixes (SQL downloads whose query is never re-issued — see below) and anything stranded by a crashed dump.

### SQL download (`GET /datastore/api/dump/query`)

`GET /datastore/api/dump/query?sql=<SELECT…>&format=csv|gzip|ndjson|parquet` exports the result of an arbitrary vetted SELECT through the same pipeline as `/datastore/api/dump/{resource_id}` — engine method `dump_sql` in [bigquery/export.py](datastore/infrastructure/engines/bigquery/export.py), response shaping shared via `download_response` in [api/endpoints/dump.py](datastore/api/endpoints/dump.py) (302 for the composed file · gzip streamed · JSON URL list for multi-file parquet). Same SQL validation + per-table auth as `datastore_search_sql` (`DatastoreDumpSQLRequest` subclasses its request schema); the action API itself stays pure JSON envelope. The route is declared before `/datastore/api/dump/{resource_id}`, making `query` a reserved resource name on the dump family.

Deltas vs the whole-table dump:

- **LIMIT is optional, uncapped.** `datastore_search_sql` requires a LIMIT literal; the dump request schema relaxes it (`parse_sql_pagination(require_limit=…)` via the `_REQUIRE_LIMIT` class flag), honors a present LIMIT as written, and `SEARCH_RESULT_ROWS_MAX` does not apply. OFFSET without LIMIT is rejected.
- **Cache key** = `dumps/<qhash>/<fmt>/<rev>/`: `qhash` = sha256 of the qualified SQL, `rev` = sha256 over every referenced table's `(rid, modified)` pair — any table change → new rev. Query dumps share the `dumps/` root with table dumps; the identity segment is a 16-hex hash rather than a resource id, so the two only collide if a table is literally named like one.
- **Non-deterministic SQL bypasses the cache.** Queries calling `now()`, `current_date`, … (`_NON_DETERMINISTIC_SQL_FUNCTIONS`) skip the lookup and export under a fresh uuid rev per run.
- **RO dry run → RW export.** The user SQL is dry-run on the RO client first (free; clean 400 on SQL that doesn't compile; yields the output schema for the same per-format casts `dump()` uses — ISO timestamps for CSV/NDJSON, `TO_JSON_STRING` for JSON→parquet). The `EXPORT DATA` itself must run under the RW SA (it writes GCS objects); containment = single-statement/SELECT-only schema validation + per-table authorize + function allow-list + the user SQL riding in subquery position (`AS SELECT … FROM (<sql>)`).
- **Age-gated GC.** Stale revisions under `dumps/<qhash>/<fmt>/` are deleted only once older than the signed-URL expiry, so a re-export can't kill shards whose URLs are still live.
- **Row order** (`_outer_order_by`): BigQuery ignores a subquery's ORDER BY without LIMIT, so ordering lives on the **outer** exported query — a user's top-level `ORDER BY` is hoisted there when its keys are output columns; with no ORDER BY, `ORDER BY _id` is applied when `_id` is in the output (mirrors JSON mode's `default_order_by`). Otherwise the file is unordered. BigQuery preserves outer ORDER BY globally across shards; shards concat in name order.
- Every cache miss is a **billed query** (EXPORT DATA never uses BigQuery's result cache); `maximum_bytes_billed` is a possible future cost cap.

The GCS client is built with the same credentials as the BigQuery client for the active engine mode (`load_credentials(config, mode)` in [bigquery/client.py](datastore/infrastructure/engines/bigquery/client.py)). Without this shim, a service-account JSON loaded via `BIGQUERY_CREDENTIALS_RO` would drive BigQuery but `storage.Client(...)` would silently fall back to ADC — a near-invisible identity split. Workload identity / `GOOGLE_APPLICATION_CREDENTIALS`-style setups still work because `load_credentials` returns `None` for ADC and the storage client follows the same default-credentials path.

---

## 6. Request / Response Contracts

Every response is the CKAN envelope — `help`, `success`, and either `result` or `error`. The full per-endpoint reference (request bodies, query params, worked examples, and error shapes) lives in **[API.md](API.md)**.
CKAN-style envelope: every response has `help`, `success`, and either `result` or `error`.

### 6.1 `POST /datastore/api/v2/datastore_create`

Running example: an electricity balancing-market auction-results table. Used
consistently across the rest of §6 so the request → search → info round-trip
is easy to follow.

**Request**
```json
{
  "resource_id": "balancing_auction_results_2025",
  "fields": [
    {
      "id": "auction_id",
      "type": "integer",
      "info": {
        "title": "Auction ID",
        "description": "Unique auction identifier. Stable across all products auctioned in the same market window.",
        "comment": "MANDATORY",
        "example": "144",
        "unit": "N/A"
      }
    },
    {
      "id": "product_code",
      "type": "string",
      "info": {
        "title": "Product Code",
        "description": "Product mnemonic for the balancing service (e.g. DCL, DCH, FFR).",
        "example": "DCL"
      }
    },
    {
      "id": "delivery_start",
      "type": "datetime",
      "info": {
        "title": "Delivery Start (UTC)",
        "description": "First instant of the delivery window. Stored as UTC; clients render local time.",
        "example": "2025-11-04T16:00:00Z"
      }
    },
    {
      "id": "duration_minutes",
      "type": "integer",
      "info": {
        "title": "Delivery Duration",
        "description": "Length of the delivery window.",
        "unit": "minutes",
        "example": "30"
      }
    },
    {
      "id": "clearing_price_gbp_per_mwh",
      "type": "number",
      "info": {
        "title": "Clearing Price",
        "description": "Pay-as-cleared price for the auction. Negative values are possible during oversupply.",
        "unit": "GBP/MWh",
        "example": "47.82"
      }
    },
    {
      "id": "volume_mwh",
      "type": "number",
      "info": {
        "title": "Cleared Volume",
        "description": "Total volume cleared in this auction.",
        "unit": "MWh",
        "example": "120.0"
      }
    },
    {
      "id": "accepted",
      "type": "boolean",
      "info": {
        "title": "Accepted",
        "description": "Whether the bid cleared (true) or was rejected (false)."
      }
    },
    {
      "id": "bidder_metadata",
      "type": "object",
      "info": {
        "title": "Bidder Metadata",
        "description": "Free-form provider-specific metadata captured at submission time.",
        "comment": "Schema not enforced; kept opaque for downstream analytics."
      }
    }
  ],
  "unique_key": ["auction_id", "product_code"],
  "records": [
    {
      "auction_id": 144,
      "product_code": "DCL",
      "delivery_start": "2025-11-04T16:00:00Z",
      "duration_minutes": 30,
      "clearing_price_gbp_per_mwh": 47.82,
      "volume_mwh": 120.0,
      "accepted": true,
      "bidder_metadata": {"unit_id": "DRAX-1", "submission_lag_ms": 412}
    },
    {
      "auction_id": 144,
      "product_code": "DCH",
      "delivery_start": "2025-11-04T16:00:00Z",
      "duration_minutes": 30,
      "clearing_price_gbp_per_mwh": 51.10,
      "volume_mwh": 75.5,
      "accepted": true,
      "bidder_metadata": {"unit_id": "EDF-COTT-2", "submission_lag_ms": 280}
    }
  ]
}
```

- `resource_id` — SQL identifier, required.
- `fields` — non-empty; each entry contains:
  - `id` (or alias `name`) — column identifier; SQL-safe.
  - `type` — column type. Accepts Frictionless canonical (`integer`, `number`, `string`, `boolean`, `date`, `datetime`, `time`, `object`, `array`, `geopoint`, `geojson`, `any`) or SQL aliases (`int4`, `int8`, `bigint`, `varchar`, `text`, `float`, `double`, `numeric`, `bool`, `timestamp`, `json`, …) which are normalised to canonical on storage.
  - `info` — optional **data dictionary** for documentation. Free-form object; recognised keys: `title`, `description`, `comment`, `example`, `unit`, plus any custom metadata. Stored verbatim and round-tripped on `datastore_info`. The outer `type` is canonical; any `info.type` is treated as a hint and ignored. Whitespace in string values is trimmed.
- `unique_key` — string or list of strings; all entries must reference declared field ids. The example uses a composite key (`auction_id` + `product_code`) since one auction clears multiple products.
- `records` — optional; each record's keys must be a subset of declared field ids.
- `primary_key` — accepted for back-compat; emits deprecation warning.

**Response — 200**
```json
{
  "help": "<request URL>",
  "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "fields": [
      {"id": "auction_id",                 "type": "integer",  "info": {"title": "Auction ID", "...": "..."}},
      {"id": "product_code",               "type": "string",   "info": {"...": "..."}},
      {"id": "delivery_start",             "type": "datetime", "info": {"...": "..."}},
      {"id": "duration_minutes",           "type": "integer",  "info": {"...": "..."}},
      {"id": "clearing_price_gbp_per_mwh", "type": "number",   "info": {"...": "..."}},
      {"id": "volume_mwh",                 "type": "number",   "info": {"...": "..."}},
      {"id": "accepted",                   "type": "boolean",  "info": {"...": "..."}},
      {"id": "bidder_metadata",            "type": "object",   "info": {"...": "..."}}
    ],
    "primary_key": ["auction_id", "product_code"],
    "unique_key": ["auction_id", "product_code"]
  }
}
```

Optional response fields (omitted from the body when not requested):
- `records` — echoes the input rows back when the request sets `include_records: true`.
- `total` — total row count after the write, populated when `include_total: true`.

### 6.2 `GET /datastore/api/v2/datastore_search`

**Query params**
| Name | Type | Default | Notes |
|---|---|---|---|
| `resource_id` | str | — | required unless `q` supplied |
| `filters` | JSON-encoded object | `null` | `{"col": value}` or `{"col": [v1, v2]}` |
| `q` | str / JSON | `null` | full-text or per-column |
| `distinct` | bool | `false` | |
| `plain` | bool | `true` | |
| `language` | str | `"english"` | reserved |
| `limit` | int | `1000` | clamped to `[0, 10000]` |
| `offset` | int | `0` | |
| `fields` | comma-separated list | all | |
| `sort` | str | `null` | `"col asc, col2 desc"` |
| `include_total` | bool | `true` | runs `COUNT(*)` if true |
| `records_format` | str | `"objects"` | `objects` / `lists` / `csv` / `tsv` |

**Example request**

```
GET /datastore/api/v2/datastore_search
    ?resource_id=balancing_auction_results_2025
    &filters={"product_code": "DCL", "accepted": true}
    &sort=delivery_start desc, clearing_price_gbp_per_mwh asc
    &fields=auction_id,product_code,delivery_start,clearing_price_gbp_per_mwh,volume_mwh
    &limit=100
    &offset=0
```

**Response (records_format=objects) — streamed**
```json
{
  "help": "...",
  "success": true,
  "result": {
    "fields": [
      {"id": "auction_id",                 "type": "integer"},
      {"id": "product_code",               "type": "string"},
      {"id": "delivery_start",             "type": "datetime"},
      {"id": "clearing_price_gbp_per_mwh", "type": "number"},
      {"id": "volume_mwh",                 "type": "number"}
    ],
    "records_format": "objects",
    "records": [
      {"auction_id": 152, "product_code": "DCL", "delivery_start": "2025-11-05T18:30:00Z", "clearing_price_gbp_per_mwh": 39.40, "volume_mwh": 95.0},
      {"auction_id": 144, "product_code": "DCL", "delivery_start": "2025-11-04T16:00:00Z", "clearing_price_gbp_per_mwh": 47.82, "volume_mwh": 120.0}
    ],
    "total": 2,
    "_links": {
      "start": "https://example.com/datastore/api/v2/datastore_search?resource_id=balancing_auction_results_2025&limit=100",
      "next":  "https://example.com/datastore/api/v2/datastore_search?resource_id=balancing_auction_results_2025&limit=100&offset=100"
    }
  }
}
```

`_links` carries the same scheme + host as the request URL, with all
non-`offset` params preserved. `start` omits `offset` (it defaults to 0);
`next` advances `offset` by `limit`. Clients detect end-of-data by an
empty `records` array on the next page — there's no `prev` field today.

`records_format=lists` returns each record as a positional array (column order matches `fields`).
`records_format=csv` / `tsv` return a streaming text body of data rows (no header row — column names are on `fields`).
`result.records_format` echoes back the format that was applied (always `objects` for
`datastore_search_sql`), so a client can tell which `records` shape it got.

### 6.3 `POST /datastore/api/v2/datastore_upsert`

**Request — late-arriving correction to an auction result**
```json
{
  "resource_id": "balancing_auction_results_2025",
  "method": "upsert",
  "unique_key": ["auction_id", "product_code"],
  "records": [
    {
      "auction_id": 144,
      "product_code": "DCL",
      "delivery_start": "2025-11-04T16:00:00Z",
      "duration_minutes": 30,
      "clearing_price_gbp_per_mwh": 48.05,
      "volume_mwh": 120.0,
      "accepted": true,
      "bidder_metadata": {"unit_id": "DRAX-1", "submission_lag_ms": 412, "revision": 2}
    },
    {
      "auction_id": 153,
      "product_code": "FFR",
      "delivery_start": "2025-11-05T19:00:00Z",
      "duration_minutes": 60,
      "clearing_price_gbp_per_mwh": 32.40,
      "volume_mwh": 200.0,
      "accepted": false,
      "bidder_metadata": {"unit_id": "SSE-PEH-3", "rejection_reason": "above_cap"}
    }
  ],
  "include_records": false,
  "include_total": false,
  "force": false
}
```

- `method`: `upsert` | `insert` | `update`. The table's stored `unique_key` (set at `datastore_create`) decides which rows match — the request body itself never carries it.
- `include_records`: if `true`, echoes the written rows back in the response.
- `include_total`: if `true`, the engine runs a `COUNT(*)` after the write and populates `result.total`. Off by default.
- `force`: bypasses optional client-side guards (reserved; backend-specific).

**Response**
```json
{
  "help": "...",
  "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "method": "upsert"
  }
}
```

Optional fields appear in `result` only when requested:

- `records` — echoes input rows when `include_records: true`.
- `total` — total row count after the write when `include_total: true`.

`null` is never serialised — fields that aren't populated are simply omitted (see `_orjson_default` in `api/responses.py`).

### 6.4 `GET /datastore/api/v2/datastore_search_sql`

**Query params**: `sql` (required; must carry a `LIMIT` literal). To export the result as a file instead of the JSON envelope, use `GET /datastore/api/dump/query?sql=…&format=…` (LIMIT optional + uncapped there — see §5.3 "SQL download").

**Example request — daily clearing-price summary**
```
GET /datastore/api/v2/datastore_search_sql?sql=
  SELECT
    DATE(delivery_start)            AS delivery_date,
    product_code,
    AVG(clearing_price_gbp_per_mwh) AS avg_price,
    SUM(volume_mwh)                 AS total_volume
  FROM balancing_auction_results_2025
  WHERE accepted = true
    AND delivery_start >= '2025-11-01'
  GROUP BY delivery_date, product_code
  ORDER BY delivery_date DESC, product_code
&limit=10000
```

**Response — streamed**
```json
{
  "help": "...",
  "success": true,
  "result": {
    "fields": [
      {"id": "delivery_date", "type": "date"},
      {"id": "product_code",  "type": "string"},
      {"id": "avg_price",     "type": "number"},
      {"id": "total_volume",  "type": "number"}
    ],
    "records_format": "objects",
    "records": [
      {"delivery_date": "2025-11-05", "product_code": "DCL", "avg_price": 41.20, "total_volume": 1840.0},
      {"delivery_date": "2025-11-05", "product_code": "DCH", "avg_price": 49.75, "total_volume":  720.5},
      {"delivery_date": "2025-11-04", "product_code": "DCL", "avg_price": 47.82, "total_volume": 1200.0}
    ],
    "records_truncated": false
  }
}
```

### 6.5 `POST /datastore/api/v2/datastore_delete`

**Request — purge rejected bids for a single auction window**
```json
{
  "resource_id": "balancing_auction_results_2025",
  "filters": {
    "auction_id": 144,
    "accepted": false
  },
  "force": false
}
```
Empty `filters` (or omitted) → the entire table is dropped. Passing `fields`
(mutually exclusive with `filters`) drops those columns instead of rows.

**Response**
```json
{
  "help": "...",
  "success": true,
  "result": {"resource_id": "balancing_auction_results_2025"}
}
```

When `fields` is supplied (column drop), `result` also carries `schema` — the
Frictionless Table Schema after the listed columns were removed — so the caller
can confirm the table's new shape without a follow-up `datastore_info`:

```json
{
  "help": "...",
  "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "fields": ["bidder_metadata"],
    "schema": {"fields": [{"id": "auction_id", "type": "integer"}, "..."], "primaryKey": ["auction_id", "product_code"]}
  }
}
```

### 6.6 `GET /datastore/api/v2/datastore_info`

Returns the same field shape that was supplied to `datastore_create`, including
the `info` data dictionary verbatim — clients can use this as a column-level
metadata catalog (titles, descriptions, units, examples) without a side store.

**Response**
```json
{
  "help": "...",
  "success": true,
  "result": {
    "resource_id": "balancing_auction_results_2025",
    "fields": [
      {
        "id": "auction_id",
        "type": "integer",
        "info": {
          "title": "Auction ID",
          "description": "Unique auction identifier. Stable across all products auctioned in the same market window.",
          "comment": "MANDATORY",
          "example": "144",
          "unit": "N/A"
        }
      },
      {
        "id": "product_code",
        "type": "string",
        "info": {
          "title": "Product Code",
          "description": "Product mnemonic for the balancing service (e.g. DCL, DCH, FFR).",
          "example": "DCL"
        }
      },
      {
        "id": "delivery_start",
        "type": "datetime",
        "info": {
          "title": "Delivery Start (UTC)",
          "description": "First instant of the delivery window. Stored as UTC; clients render local time.",
          "example": "2025-11-04T16:00:00Z"
        }
      },
      {"id": "duration_minutes",           "type": "integer", "info": {"title": "Delivery Duration", "unit": "minutes"}},
      {"id": "clearing_price_gbp_per_mwh", "type": "number",  "info": {"title": "Clearing Price",    "unit": "GBP/MWh"}},
      {"id": "volume_mwh",                 "type": "number",  "info": {"title": "Cleared Volume",    "unit": "MWh"}},
      {"id": "accepted",                   "type": "boolean", "info": {"title": "Accepted"}},
      {"id": "bidder_metadata",            "type": "object",  "info": {"title": "Bidder Metadata"}}
    ],
    "unique_key": ["auction_id", "product_code"],
    "primary_key": ["auction_id", "product_code"],
    "total": 18420
  }
}
```

### 6.7 Error envelope (all 4xx / 5xx)

```json
{
  "help": "<request URL>",
  "success": false,
  "error": {
    "__type": "Validation Error",
    "message": "fields[0].id is not a valid identifier: '1bad'",
    "fields": {"fields": ["..."]}    // optional, present on validation errors
  }
}
```

`__type` taxonomy: `Validation Error` (400), `Authorization Error` (403), `Not Found Error` (404), `Conflict Error` (409), `Internal Error` (500).


## 7. Roadmap

The original phase plan that used to live here has mostly shipped. This section now tracks what's done, what's next, and the guardrails that apply to every change. For the current file layout see §3.

### Done

- [x] **Foundation** — `pyproject.toml`, `Dockerfile`, `Makefile`, `.env.example`, `docker-compose.yml`. App factory + lifespan in [datastore/main.py](datastore/main.py); body-size middleware in [datastore/api/middleware.py](datastore/api/middleware.py); startup log line via `uvicorn.error` showing the active engine + auth provider + cache backend.
- [x] **All six `datastore_*` actions wired** — `create`, `upsert`, `delete`, `search`, `search_sql`, `info` mounted via [datastore/api/routes.py](datastore/api/routes.py). Every endpoint authorizes via `context.authorize(...)` and delegates to a service.
- [x] **Real BigQuery backend** — [datastore/infrastructure/engines/bigquery/](datastore/infrastructure/engines/bigquery/) implements DDL, parameterised `search`, MERGE-based `upsert` (`method=upsert` / `insert` / `update`), DML `delete` (whole-table drop, row delete, column drop), parameterised `search_sql`, and `info`. Frictionless schema + `unique_key` round-trip via native table-level metadata — JSON-encoded into the table's own `description` OPTION (no separate metadata table). Row counts use the cheap `INFORMATION_SCHEMA.TABLE_STORAGE` fast path when filters don't apply.
- [x] **Streaming search** — [datastore/services/streaming.py](datastore/services/streaming.py) yields the CKAN envelope chunk-by-chunk for all four `records_format` values (`objects`, `lists`, `csv`, `tsv`); CSV/TSV ride the same JSON envelope (records is a multi-line string). Peak memory ≈ 1 row regardless of N. `_links.start` / `_links.next` carry full scheme + host with all non-`offset` params preserved.
- [x] **`datastore_search_sql` SQL safety** — schema rejects non-SELECT / multi-statement / unparseable SQL (sqlglot). [datastore/schemas/validators.py](datastore/schemas/validators.py)'s `parse_sql_references` pulls table + function names; endpoint authorizes each table as a `resource_id`; service rejects functions outside the engine's allow-list at `engines/<name>/allowed_functions.txt` (overridable via `SQL_FUNCTIONS_ALLOW_FILE`).
- [x] **Request validation** — Pydantic models in [datastore/schemas/request.py](datastore/schemas/request.py) with `extra="forbid"`. `datastore_info` / `datastore_delete` accept `resource_id` or `id` (normalised). Pydantic errors → CKAN error envelope with a `fields` map.
- [x] **Response models** — [datastore/schemas/responses.py](datastore/schemas/responses.py) — one envelope per endpoint with a nested `Result` class. Routes declare `response_model=...` for OpenAPI; services return the typed inner `Result`.
- [x] **Error envelope** — handlers in [datastore/api/error_handlers.py](datastore/api/error_handlers.py); taxonomy in [datastore/core/exceptions.py](datastore/core/exceptions.py).
- [x] **Pluggable auth providers** — `AUTH_TYPE` selects a folder under [datastore/auth/](datastore/auth/). Built-in: `ckan` (delegates to `datastore_authorize` with a provider-local TTL cache), `jwt` (PyJWT verify HS*/RS*/ES* + `aud`/`iss`/`exp`), `anonymous` (allow-all). Boundary policy in [datastore/api/auth.py](datastore/api/auth.py) is provider-agnostic. Adding a new provider = drop a folder; no registry / config edit.
- [x] **Standalone capability** — `CKANClient` is only constructed when `AUTH_TYPE=ckan`; `RequestContext.ckan` is `CKANClient | None`. `Config` validator rejects `AUTH_TYPE=ckan` + empty `CKAN_URL` at startup. `datastore_create` `resource` dict path is gated on CKAN auth; everything else runs without an upstream CKAN.
- [x] **`/datastore/api/ready` healthcheck** — lifespan builds rw + ro engine instances and stashes on `app.state`; `/datastore/api/ready` calls `engine.healthcheck()` on both and returns 503 + `Service Unavailable` envelope if either fails.
- [x] **Request context** — `RequestContext` + `ContextDep` in [datastore/api/context.py](datastore/api/context.py); CKAN client bound to the caller's `api_key` per request (or `None` under non-CKAN auth). `.authorize()` method delegates to `api/auth.py` policy + active provider.
- [x] **Engine + auth registries** — `DatastoreBackend` ABC + result dataclasses in [engines/base.py](datastore/infrastructure/engines/base.py); `AuthProvider` Protocol + `Decision` in [auth/base.py](datastore/auth/base.py). Each subpackage exports `Backend` / `Provider`; `DATASTORE_ENGINE` / `AUTH_TYPE` are validated against directories on disk at startup; registries dispatch via `importlib`.
- [x] **Themed Swagger UI** — `/datastore/api/v2/docs` is served by `_register_swagger_docs` in [datastore/main.py](datastore/main.py), not FastAPI's stock route. The page is a Jinja template at [datastore/api/templates/docs.html](datastore/api/templates/docs.html), so autoescaping is structural — though it covers the HTML contexts only, which is why the colours are validated at config load and the spec URL is emitted through `tojson`. Swagger UI is **vendored** under [datastore/api/static/](datastore/api/static/) (swagger-ui-dist 5.17.14) and mounted at `/datastore/api/static`, so the page renders with no CDN and no outbound network. The stylesheet is ported from `ckanext-openapidocs` so this service's docs and the CKAN portal's read as one family: Swagger's own topbar, servers dropdown and duplicate title block are hidden in favour of a branded header, and the Authorize dialog is restyled (900px wide, so a pasted JWT / API key is readable in full). Branding comes from `DOCS_PRIMARY_COLOR` / `DOCS_HEADER_COLOR` / `DOCS_SITE_TITLE` / `DOCS_LOGO_URL`, written into CSS custom properties — the colours are validated as CSS colours at config load, since they land inside a `<style>` block. Empty values keep the stylesheet's defaults.
- [x] **Postman collection** — [postman/collection.json](postman/collection.json) auto-generated from `example_payload/` by `postman/generate_postman.py`; covers every endpoint with a worked example.
- [x] **Tests** — ~290 tests across endpoint, service, auth provider, and engine layers. CKAN pytest plugin disabled via `addopts` in `pyproject.toml`.

### Next

Rough priority order. Tick each box as the change set lands.

- [ ] **DuckLake backend.** Second concrete engine implementing `DatastoreBackend`. Single-replica `StatefulSet` + `PersistentVolumeClaim` in k8s. Local mode reads `DUCKDB_PATH`; DuckLake mode reads a catalog URL.
- [ ] **Observability.** JSON structured logger in `core/logging.py`; per-request middleware in `api/middleware.py` injects a `request_id` and logs `method`, `path`, `status`, `duration_ms`. The existing `log.debug` lines in auth + error handlers + the CKAN provider light up under `LOG_LEVEL=DEBUG`.
- [ ] **Per-table SQL auth for `datastore_search_sql`** — today the endpoint authorizes each table the schema extracts via the active provider, but CKAN's `datastore_search_sql_authorize` is a separate action that takes the SQL string. Wire it through `context.ckan` for the CKAN provider as a tighter check; JWT / anonymous providers stay table-by-table.
- [ ] **Opt-in query-result cache.** The CKAN auth provider already caches its own decisions. A separate cache for small / hot SELECTs would ride on the existing `CachePort`. Not on the critical path — defer until there's a workload that needs it.
- [ ] **`terminationGracePeriodSeconds: 30`** in the k8s manifest so streaming responses drain on SIGTERM.

### Guardrails

Apply to every change, current and future:

| Invariant | Check |
|---|---|
| App starts | `uvicorn datastore.main:app` exits 0 |
| Health always works | `GET /datastore/api/health` → 200 |
| OpenAPI loads | `GET /datastore/api/v2/docs` renders without error |
| Tests stay green | `pytest` passes |
| Layer arrow holds | `rg "from (fastapi\|starlette)" datastore/services datastore/infrastructure datastore/core` returns nothing |

Hard rules from §3 (recap):

- Only `datastore/api/` and `datastore/main.py` may import from `fastapi` / `starlette`.
- Only `datastore/schemas/` and `datastore/core/config.py` may import from `pydantic` / `pydantic_settings`.
- Engines return lazy row iterators of tuples (when streaming lands). Never `list[dict]`.
- Pydantic validates at the boundary; orjson serialises out via `_success_response`.
- No DI container — FastAPI's `Depends` + the engine `registry.py` factory are the only wiring.
