"""BigQuery `Client` construction.

Credentials come from `BIGQUERY_CREDENTIALS` (read-write) or
`BIGQUERY_CREDENTIALS_RO` (read-only, falls back to read-write). The
value is either a JSON blob (leading `{`) or a path to a service-account
JSON file. Empty → Application Default Credentials.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from google.cloud import bigquery

    from datastore.core.config import Config

Mode = Literal["rw", "ro"]


def build_client(config: Config, mode: Mode) -> bigquery.Client:
    from google.cloud import bigquery

    project = config.BIGQUERY_PROJECT.strip()
    if not project:
        raise RuntimeError(
            "BIGQUERY_PROJECT is required when DATASTORE_ENGINE=bigquery"
        )

    creds_raw = (
        config.BIGQUERY_CREDENTIALS_RO
        if mode == "ro"
        else config.BIGQUERY_CREDENTIALS
    ).strip()

    if not creds_raw:
        return bigquery.Client(project=project)
    return bigquery.Client(
        project=project, credentials=_credentials_from_raw(creds_raw)
    )


def _credentials_from_raw(raw: str):
    from google.oauth2 import service_account

    if raw.startswith("{"):
        return service_account.Credentials.from_service_account_info(
            json.loads(raw)
        )
    return service_account.Credentials.from_service_account_file(raw)
