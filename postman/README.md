# Postman collection

Postman v2.1.0 collection covering every Datastore API endpoint.
Auto-generated from [`example_payload/`](../example_payload/) by
[`generate_postman.py`](generate_postman.py).

## Import

In Postman: **File → Import** → `collection.json`. Seven folders appear:
`health`, `datastore_create`, `datastore_upsert`, `datastore_info`,
`datastore_search`, `datastore_search_sql`, `datastore_delete`.

## Variables

| Variable     | Default                          | Notes                                    |
|--------------|----------------------------------|------------------------------------------|
| `baseUrl`    | `http://localhost:8000`          | Datastore API root.                      |
| `apiKey`     | (empty)                          | CKAN API key — required for writes.      |
| `resourceId` | `balancing_auction_results_2025` | Resource to create / write / query.      |

Collection auth sends `Authorization: {{apiKey}}` on every request.

## Walkthrough

Run folders top-to-bottom on a fresh resource:

1. **`datastore_create`** — seeds **110 rows** (auctions `1..55` × `DCL`/`DCH`).
2. **`datastore_upsert`** — upsert 2 → insert 10 (`100..109`) → update 2. Total: 121.
3. **`datastore_info`** — confirm schema + row count.
4. **`datastore_search`** — filter / full-text / paginated.
5. **`datastore_search_sql`** — raw SQL; `LIMIT` required. JOIN/UNION variants need a second resource `balancing_auction_results_2024`.
6. **`datastore_delete`** — row delete (`auction_id=1`) → drop column (`bidder_metadata`) → drop table.

`health` is independent — hit any time to check the server.

## Regenerate

```sh
python postman/generate_postman.py
```

Drop new files under `example_payload/<action>/<name>.json` to add requests.

## Auth

- Reads can run anonymously (CKAN decides by resource visibility).
- Writes need `apiKey`, or set `AUTH_ENABLED=false` in `.env` for local dev.
