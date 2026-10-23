"""Pydantic schemas used ONLY by the legacy CKAN surface.

DEPRECATED — see `datastore/legacy/__init__.py`.

Lives here (not `datastore/schemas/request.py`) so the v2 request
schemas don't carry back-compat subclasses after legacy is removed.
"""

from __future__ import annotations

from typing import ClassVar

from datastore.schemas.request import DatastoreSearchSQLRequest


class DatastoreLegacySearchSQLRequest(DatastoreSearchSQLRequest):
    """Legacy-API variant of `datastore_search_sql`.

    Real CKAN's `datastore_search_sql` accepts SQL without a LIMIT clause —
    customer scripts in the wild rely on that. Flip `_REQUIRE_LIMIT` off
    so a bare `SELECT * FROM "<rid>"` parses; every other guard (SELECT-
    only, single statement, function allow-list, per-table authorize) is
    inherited unchanged.
    """

    _REQUIRE_LIMIT: ClassVar[bool] = False
