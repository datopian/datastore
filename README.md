# Datastore API

CKAN-compatible datastore API with a pluggable storage backend (BigQuery or DuckDB).

See [CLAUDE.md](CLAUDE.md) for the full architecture, API contracts, and phased execution plan.

## Dev setup

Requires Python 3.12+.

```sh
# Install dependencies (pick one)
pip install -r requirements.txt -r requirements-dev.txt   # plain pip
pip install -e ".[dev]"                                   # editable; reads the same requirements files

# Run dev server
uvicorn app.main:app --reload

# Open API docs
open http://localhost:8000/docs

# Run tests
pytest
```

Dependency lists live in [requirements.txt](requirements.txt) (runtime) and [requirements-dev.txt](requirements-dev.txt) (test/lint/type tools). `pyproject.toml` reads them dynamically — edit the `.txt` files, not the TOML.

## Env vars

| Name | Default | Purpose |
|---|---|---|
| `APP_MESSAGE` | `"Welcome to the Datastore API"` | Banner returned by `GET /` |
| `MAX_REQUEST_BODY_MB` | `50` | Reject larger bodies |
| `DATASTORE_BACKEND` | `bigquery` | `bigquery` or `duckdb` |
| `BQ_PROJECT` | _(none)_ | BigQuery project |
| `AUTH_REDIS_URL` | _(none)_ | Redis URL for auth cache |
| `LOG_LEVEL` | `INFO` | Stdlib logging level |
