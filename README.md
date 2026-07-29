# Datastore API

A **standalone datastore service** for tabular data. It provides a simple API
for creating tables, inserting / updating / deleting rows, and searching them
with filters or SQL. It can serve as a CKAN datastore or run independently.

Storage backends and auth providers are pluggable and easy to extend:
- **Pluggable storage** — `DATASTORE_ENGINE` selects a backend (BigQuery today; DuckLake planned).
- **Pluggable auth** — `AUTH_TYPE` selects a provider: `ckan` / `jwt` / `anonymous`.

## Quick start

Requires Python 3.12+.

```sh
pip install -e ".[dev]"          # install (editable, with dev tools)
uvicorn datastore.main:app --reload   # run dev server
pytest                            # run tests
```

Open `http://localhost:8000/datastore/api/docs` for interactive API docs.

## Configuration

All settings are environment variables mapping 1:1 to `datastore.core.config.Config`.
Copy [.env.example](.env.example) and fill it in. The essentials:

| Var | Default | Purpose |
|---|---|---|
| `DATASTORE_ENGINE` | `bigquery` | Storage backend (folder under `datastore/infrastructure/engines/`) |
| `AUTH_TYPE` | `ckan` | Auth provider: `ckan` · `jwt` · `anonymous` |
| `CKAN_URL` | — | CKAN base URL (required when `AUTH_TYPE=ckan`) |
| `BIGQUERY_PROJECT` / `BIGQUERY_DATASET` | — | Required when `DATASTORE_ENGINE=bigquery` |
| `BIGQUERY_EXPORT_BUCKET` | — | GCS bucket for downloads (`/datastore/dump/{resource_id}`, `/datastore/dump/query`) |
| `REDIS_URL` | — | Cache backend; empty → in-process cache |

**Note on the export bucket:** everything the service writes lives under
a single `dumps/` prefix — `dumps/<resource_id>/…` for whole-table downloads
and `dumps/<query-hash>/…` for SQL downloads — so one lifecycle rule covers
everything. Configure a ~24h rule on
`BIGQUERY_EXPORT_BUCKET`: the engine garbage-collects superseded export
revisions itself, but abandoned query-download prefixes (queries never
re-issued) and files stranded by a crashed export are only ever cleaned by
the lifecycle rule.

## Documentation

- **[API.md](API.md)** — full API reference (endpoints, request/response, examples).
- **`GET /datastore/api/docs`** — interactive Swagger UI (also `/datastore/api/redoc` and `/datastore/api/openapi.json`).
- **[CLAUDE.md](CLAUDE.md)** — architecture, design decisions, and layout.

## License

See repository.
