from __future__ import annotations

from fastapi import APIRouter

from datastore.api.endpoints import datastore, dump, health
from datastore.core.constants import API_BASE_PREFIX, API_PREFIX, DUMP_PREFIX

# DEPRECATED legacy CKAN surface. These two imports and the two
# `legacy_endpoints` `include_router` calls below are the ONLY references
# to `datastore.legacy` in the entire v2 codebase — remove them (plus
# `datastore/legacy/` and `tests/test_legacy_api.py`) when the legacy URLs
# retire. See `datastore/legacy/__init__.py` for the removal plan.
from datastore.legacy import endpoints as legacy_endpoints
from datastore.legacy.constants import LEGACY_API_PREFIX, LEGACY_DUMP_PREFIX

api_router = APIRouter()
# Unversioned: a probe URL is stable regardless of the action contract.
api_router.include_router(health.probe_router, prefix=API_BASE_PREFIX)
# Downloads: versioned alongside the actions, since their query params and
# output layout are part of the same contract. Mounted before the action
# router so `/dump/...` can't be read as an action name.
api_router.include_router(dump.router, prefix=DUMP_PREFIX)
# Versioned: the datastore actions are the compatibility contract.
api_router.include_router(datastore.router, prefix=API_PREFIX)

# DEPRECATED — see `datastore/legacy/__init__.py`. These two mounts
# expose read-only CKAN-compatible URLs so scripts in the wild keep
# working while callers migrate to v2.
api_router.include_router(legacy_endpoints.router, prefix=LEGACY_API_PREFIX)
api_router.include_router(legacy_endpoints.dump_router, prefix=LEGACY_DUMP_PREFIX)
