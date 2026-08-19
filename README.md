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

Open `http://localhost:8000/datastore/api/v2/docs` for interactive API docs.

## Configuration

All settings are environment variables mapping 1:1 to `datastore.core.config.Config`.
Copy [.env.example](.env.example) and fill it in. The essentials:

| Var | Default | Purpose |
|---|---|---|
| `DATASTORE_ENGINE` | `bigquery` | Storage backend (folder under `datastore/infrastructure/engines/`) |
| `AUTH_TYPE` | `ckan` | Auth provider: `ckan` · `jwt` · `anonymous` |
| `CKAN_URL` | — | CKAN base URL (required when `AUTH_TYPE=ckan`) |
| `BIGQUERY_PROJECT` / `BIGQUERY_DATASET` | — | Required when `DATASTORE_ENGINE=bigquery` |
| `BIGQUERY_EXPORT_BUCKET` | — | GCS bucket for downloads (`/datastore/api/dump/{resource_id}`, `/datastore/api/dump/query`) |
| `REDIS_URL` | — | Cache backend; empty → in-process cache |
| `DOCS_PRIMARY_COLOR` / `DOCS_HEADER_COLOR` | — | Swagger UI branding (see [Documentation](#documentation)) |
| `DOCS_SITE_TITLE` / `DOCS_LOGO_URL` | — | Docs page header title and logo |

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
- **`GET /datastore/api/v2/docs`** — interactive Swagger UI (also `/datastore/api/v2/redoc` and `/datastore/api/v2/openapi.json`).

Swagger UI is **vendored** (no CDN), so the docs page works with no outbound
network access. It shares its stylesheet with `ckanext-openapidocs`, so this
service's docs and the CKAN portal's look like one family. Rebrand it with env
vars rather than CSS:

```sh
DOCS_PRIMARY_COLOR=#7A3864      # links, inline code, Authorize/Execute buttons
DOCS_SITE_TITLE="NESO Datastore API"
DOCS_LOGO_URL=/static/logo.png
```

`DOCS_PRIMARY_COLOR` alone brands the whole page: the header bar inherits it,
as do links, inline code and the Authorize/Execute buttons. Set
`DOCS_HEADER_COLOR` only to decouple the bar from the accent — a neutral
`#1f2937` stops a saturated brand colour competing with the content:

```sh
DOCS_HEADER_COLOR=#1f2937       # neutral bar, brand-coloured accents
```

Both colours accept any CSS colour and are validated at startup, since they
land inside a `<style>` block. Empty values keep the stylesheet's defaults.
HTTP method badges keep their conventional colours whatever you brand with,
so reads and writes stay distinguishable.
- **[CLAUDE.md](CLAUDE.md)** — architecture, design decisions, and layout.

## License

See repository.
