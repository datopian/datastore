"""BigQuery `Client` construction.

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

    creds = load_credentials(config, mode)
    if creds is None:
        return bigquery.Client(project=project)
    return bigquery.Client(project=project, credentials=creds)


def load_credentials(config: Config, mode: Mode = "ro"):
    """Resolve service-account credentials for this engine mode.

    Defaults to `ro` — the safer choice. Read-only paths (dump, search)
    use this on their own; `build_client` passes `mode` explicitly when
    constructing the rw client. An empty RO credential falls through
    to ADC; it **never** falls back to the RW key, which would silently
    give read paths write privileges and defeat the credential split.
    """
    creds_raw = (
        config.BIGQUERY_CREDENTIALS_RO
        if mode == "ro"
        else config.BIGQUERY_CREDENTIALS
    ).strip()
    if not creds_raw:
        return None
    return _credentials_from_raw(creds_raw)


def _credentials_from_raw(raw: str):
    from google.oauth2 import service_account

    if raw.startswith("{"):
        return service_account.Credentials.from_service_account_info(
            json.loads(raw)
        )
    return service_account.Credentials.from_service_account_file(raw)
