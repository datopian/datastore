"""BigQuery datastore backend (placeholder — real adapter pending).

Selected by `DATASTORE_ENGINE=bigquery`. Function allow-list for
`datastore_search_sql` is loaded from `allowed_functions.txt` in this
package — see `infrastructure.engines.registry.get_allowed_sql_functions`.
"""

from datastore.infrastructure.engines.bigquery.backend import BigQueryBackend

Backend = BigQueryBackend

__all__ = ["Backend", "BigQueryBackend"]
