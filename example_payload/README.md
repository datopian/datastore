# Request payload examples

Hand-written JSON bodies for the CKAN datastore endpoints. Useful as:

- copy-paste fixtures when smoke-testing a running dev server,
- canonical references when documenting clients,
- starting points when adding new tests.

## Layout

One subdirectory per endpoint; one file per **distinct scenario** (not per
field combination — keep it useful, not exhaustive).

```
example_payload/
├── datastore_create/
│   ├── with_resource_id.json     # legacy `fields` + `primary_key` — existing-resource flow
│   ├── with_resource.json        # legacy `fields` + `primary_key` — new-resource flow
│   └── with_schema.json          # frictionless `schema` (recommended) — canonical input
├── datastore_upsert/
│   ├── upsert.json               # default — corrects one row + adds a new one
│   ├── insert.json               # method=insert; new rows only
│   └── update.json               # method=update; patches existing rows by unique_key
├── datastore_delete/
│   ├── with_filters.json         # narrow delete by column values
│   ├── whole_table.json          # no filters → drop the entire table
│   └── force_readonly.json       # force=true to delete from a read-only resource
├── datastore_search/
│   ├── basic.json                # minimal — just resource_id
│   ├── with_filters.json         # narrow by column values (e.g. product_code, accepted)
│   ├── with_full_text.json       # `q` full-text query, plain + language
│   ├── paginated_sorted.json     # fields + sort + limit + offset + include_total
│   └── response.json             # sample RESPONSE for paginated_sorted.json,
│                                 # showing the CKAN envelope + `_links`
├── datastore_search_sql/
│   ├── basic.json                # SELECT with WHERE + LIMIT
│   ├── aggregate.json            # GROUP BY + AVG / SUM
│   └── with_cte.json             # WITH (CTE) + ORDER BY
└── datastore_info/
    ├── basic.json                # minimal — just resource_id
    └── with_id_alias.json        # `id` alias accepted in place of resource_id
```

## How to add a new example

Three steps:

1. **Pick the right subdirectory.** If you're adding the first example for a
   new endpoint (e.g. `datastore_delete`), create the directory with the
   endpoint's action name (`example_payload/datastore_delete/`).

2. **Name the file after the scenario.** Short, lowercase, snake_case.
   Examples: `by_filters.json`, `whole_table.json`, `empty_records.json`.
   The filename is the only label the reader sees — make it tell the story.

3. **Match the request schema.** Each endpoint has a Pydantic model in
   [datastore/schemas/request.py](../datastore/schemas/request.py).
   The payload must validate against it. (Files named `response*.json`
   are sample server responses — skip this step for those.) Quick check:

   ```sh
   python -c "
   import json
   from datastore.schemas.request import DatastoreUpsertRequest
   DatastoreUpsertRequest.model_validate(
       json.load(open('example_payload/datastore_upsert/upsert.json'))
   )
   print('OK')
   "
   ```

   Swap the model name to match the endpoint (`DatastoreCreateRequest`,
   `DatastoreUpsertRequest`, …).

## Smoke-test against a running server

```sh
# Start the dev server in another shell:
#   uvicorn datastore.main:app --reload

curl -s -X POST http://localhost:8000/api/3/action/datastore_upsert \
     -H 'Content-Type: application/json' \
     -H 'Authorization: <api-key>' \
     -d @example_payload/datastore_upsert/upsert.json | jq
```

Set `AUTH_ENABLED=false` in `.env` for local runs without a CKAN instance —
auth is bypassed and a stub decision is returned.

## Conventions

- **Realistic values.** Use the running balancing-market example (auctions,
  products, prices) so the files read as a coherent dataset across endpoints.
- **No PII, no secrets.** Treat these as public.
- **Stable resource IDs.** Reuse the same `resource_id` across files in a
  scenario chain (e.g. create → upsert → search) so a reader can follow the
  flow end-to-end.
- **One concept per file.** If you're tempted to demonstrate two unrelated
  features in one payload, split it into two files.
