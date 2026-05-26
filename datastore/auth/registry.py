"""Provider factory — dispatch by `Config.AUTH_TYPE` via importlib.

Adding a new provider = drop `datastore/auth/<name>/` with `__init__.py`
exporting `Provider = <ConcreteClass>`. No edit here.

The lifespan calls this once at startup and stores the result on
`app.state.auth_provider`; there's no instance cache here on purpose —
the only cache in the auth path is the CKAN provider's per-decision
TTL cache (see `auth/ckan/provider.py`).
"""

from __future__ import annotations

import importlib
from typing import Any

from datastore.auth.base import AuthProvider
from datastore.core.config import Config


def get_auth_provider(config: Config, **extras: Any) -> AuthProvider:
    """Construct the provider for `config.AUTH_TYPE`.

    `extras` are forwarded to the provider constructor (e.g. `ckan=`,
    `cache=`, `cache_ttl=`). Providers absorb unused kwargs via `**_`.
    """
    module = importlib.import_module(f"datastore.auth.{config.AUTH_TYPE}")
    provider: AuthProvider = module.Provider(config=config, **extras)
    return provider
