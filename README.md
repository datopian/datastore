# Datastore API

A CKAN datastore like API for tabular data storage and querying,
built on the FastAPI framework with a pluggable storage engine
(BigQuery today; DuckLake on the roadmap). Exposes
`/api/3/action/datastore_*` action endpoints.

Each request is authorised against an upstream CKAN instance via
`datastore_authorize` and TTL-cached (in-process by default; Redis when
`REDIS_URL` is set), so the heavy datastore work lives in this service
while CKAN remains the single source of truth for users, packages,
resources, and permissions.

## Project structure

```
datastore/
├── main.py                       # FastAPI app factory + lifespan
│
├── api/                          # HTTP layer — only layer that imports fastapi / starlette
│   ├── routes.py                 # Top-level APIRouter; aggregates endpoints/
│   ├── context.py                # RequestContext  (per-request DI bundle)
│   ├── auth.py                   # CKAN datastore_authorize with TTL cache
│   ├── middleware.py             # ASGI middleware (e.g. BodySizeLimitMiddleware)
│   ├── responses.py              # Envelope response helpers (_success_response / _error_response)
│   ├── error_handlers.py         # Exception handlers (APIError → CKAN error envelope)
│   └── endpoints/                # Route handlers, one file per resource group
│       ├── health.py             # /, /health, /ready
│       └── datastore.py          # /api/3/action/datastore_*
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
│   └── read.py                   # placeholder for search / search_sql / info
│
└── infrastructure/               # Adapters to outside systems
    ├── cache.py                  # InMemoryCache + RedisCache (CachePort protocol)
    ├── ckan_client.py            # CKAN action API client (httpx-backed)
    └── engines/                  # Storage backends
        ├── base.py               # DatastoreBackend ABC + result dataclasses
        ├── registry.py           # get_datastore_engine factory (picks backend by config)
        ├── bigquery.py           # BigQuery adapter
        └── ducklake.py           # DuckLake adapter (planned, not yet implemented)
```

## Roadmap

What's shipped and what's next. Tick each box as the change set lands.

### Done

- [x] Foundation (app factory, lifespan, middleware, Dockerfile, Makefile, env config)
- [x] CKAN API surface mounted at `/api/3/action/datastore_*` (`datastore_create` live; 5 others return 501)
- [x] Health endpoints `/`, `/health`, `/ready` returning the CKAN envelope shape
- [x] Strict request validation (`DatastoreCreateRequest` + `FieldSpec`)
- [x] CKAN error envelope mapping (`APIError` taxonomy + handlers)
- [x] CKAN auth gate with TTL cache (InMemory by default; Redis when `REDIS_URL` is set)
- [x] Request context bundle (`RequestContext` / `ContextDep` / bound `CKANClient`)
- [x] Service-layer separation (`create_datastore`)
- [x] Engine abstraction + factory (`DatastoreBackend` ABC + `registry.py`)
- [x] Pydantic response models with nested `Result` per endpoint
- [x] End-to-end TestClient suite + service-level unit tests

### Next

- [ ] Wire the remaining datastore endpoints (`upsert`, `delete`, `search`, `search_sql`, `info`)
- [ ] Real BigQuery backend (replace the placeholder in `infrastructure/engines/bigquery.py`)
- [ ] Streaming search responses (JSON / CSV / TSV; ≈ 1-row peak memory)
- [ ] Real `/ready` healthcheck — wire engine instances through the lifespan
- [ ] DuckLake backend (second concrete engine implementing the same ABC)
- [ ] Observability — JSON structured logs + request-id middleware
- [ ] Opt-in query-result cache (deferred until BigQuery + streaming land)


## CKAN-side requirement

This service does not implement its own user / permission model.
Every request is gated by a call to CKAN's `datastore_authorize`
action, which is **not part of stock CKAN** — it ships in the
[`ckanext-datastore-authz`](https://github.com/datopian/ckanext-datastore-authz)
extension.

Before pointing this service at a CKAN instance, install the extension
on the CKAN side and confirm the action is reachable:

```sh
curl -s "$CKAN_URL/api/3/action/datastore_authorize" \
     -H "Authorization: $CKAN_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"resource_id": "<some-resource-id>"}' | jq
```

If that returns a CKAN envelope with `success: true` and a
`result.{package, resource}` body, you're set. If you get 404, the
extension isn't installed or isn't enabled in CKAN's `ckan.plugins`.

For local dev without a CKAN at all, set `AUTH_ENABLED=false` in `.env`
— the auth gate returns a stub decision and every request passes.



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
| `DATASTORE_ENGINE` | `bigquery` | Storage backend: `bigquery` or `ducklake` |
| `BQ_PROJECT` | _(empty)_ | Google Cloud project ID for the BigQuery backend |
| `REDIS_URL` | _(empty)_ | Redis URL for cache; empty → in-process `InMemoryCache` |
| `CKAN_URL` | _(empty)_ | Base URL of the CKAN instance (required when `AUTH_ENABLED=true`) |
| `HTTP_TIMEOUT_SECONDS` | `10` | Timeout for outbound CKAN calls (seconds) |
| `AUTH_ENABLED` | `true` | CKAN auth gate; set to `false` for local dev / CI without a CKAN |
| `AUTH_CACHE_TTL` | `10` | TTL for cached `datastore_authorize` decisions (seconds) |
| `LOG_LEVEL` | `INFO` | Stdlib logging level (`DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL`) |

## API Documentation 

 http://localhost:8000/docs

## Development notes


### Adding a new endpoint

Handler in `datastore/api/endpoints/<resource>.py` (parse → call service → return CKAN envelope), request shape in `datastore/schemas/`, business logic in `datastore/services/`. Wire a new file into `datastore/api/routes.py`.


### Request context

Each endpoint takes a single `Context` that bundles the per-request handles (`auth`, `ckan`, `config`, and more as we grow). The bundle wires them together so handlers stay one-liner.

```python
from datastore.api.context import Context

@router.post("/datastore_create", response_model=DatastoreCreateResponse)
async def datastore_create(
    request: Request,
    payload: DatastoreCreateRequest,
    context: Context,
):
    # Authorize against CKAN. Pass `resource_id` (existing resource)
    # or `package_id` (new resource under that package) — exactly one.
    data_dict = await context.auth.authorize(
        resource_id=payload.resource_id,
        permission="create",        # read | create | update | delete | patch
    )

    # The service does the actual work (CKAN resource_create, engine.create, …).
    result = await create_datastore(context, data_dict)
    return _success_response(request, result)
```

- `context.auth` — `AuthContext`: cached `datastore_authorize` permission check. Holds the bound `api_key`, the cache, the TTL, and the CKAN client it delegates to.
- `context.ckan` — `CKANClient` already bound to the caller's `api_key`. Call `resource_create` / `resource_patch` / `datastore_authorize` directly; the api_key travels with the client.
- `context.config` — the loaded `Config` instance.



### Response envelopes

Every successful response follows the CKAN shape `{help, success, result}`. The base `ResponseModel` in [datastore/schemas/responses.py](datastore/schemas/responses.py) carries `help` + `success`; each endpoint subclasses it and declares an inner `Result`:

```python
class DatastoreCreateResponse(ResponseModel):
    class Result(BaseModel):
        resource_id: str
        package_id: str | None = None
        fields: list[FieldSpec]
        primary_key: list[str] = Field(default_factory=list)
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

Two layers of tests live in [tests/](tests/):

- **End-to-end** ([test_datastore_create.py](tests/test_datastore_create.py)) — uses the `client` fixture in [tests/conftest.py](tests/conftest.py), which wires up `FakeCKAN` (in-memory CKAN stand-in) and `InMemoryCache` via `app.dependency_overrides`. No real network calls.
- **Service-level** ([test_write_service.py](tests/test_write_service.py)) — calls `create_datastore` directly with a fake context. Fast, no HTTP, isolates orchestration from FastAPI plumbing.

`FakeCKAN` exposes `add_resource(...)`, `add_package(...)`, `deny(api_key)` to set up scenarios, and an `authorize_calls` counter to assert cache behaviour.

Mark slow / network-bound tests with `@pytest.mark.integration` so they can be skipped in CI by default.

The CKAN pytest plugin auto-installed system-wide is disabled for this project via `addopts = "-p no:ckan -p no:ckan_fixtures"` in `pyproject.toml` — otherwise it tries to load a CKAN `.ini` we don't have.
