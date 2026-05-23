"""Anonymous provider — every call returns an empty `Decision`."""

from __future__ import annotations

import asyncio

from datastore.auth.anonymous import Provider as AnonymousProvider
from datastore.auth.base import Decision


def test_authorize_returns_empty_decision_regardless_of_inputs() -> None:
    provider = AnonymousProvider()
    decision = asyncio.run(provider.authorize(
        credential=None,
        resource_id="any",
        package_id=None,
        permission="read",
    ))
    assert decision == Decision()


def test_authorize_does_not_care_about_credential() -> None:
    provider = AnonymousProvider()
    # Same result whether or not a token is presented.
    a = asyncio.run(provider.authorize(
        credential="token-1", resource_id="r", package_id=None, permission="read",
    ))
    b = asyncio.run(provider.authorize(
        credential=None, resource_id="r", package_id=None, permission="read",
    ))
    assert a == b == Decision()


def test_key_id_is_constant_anon_string() -> None:
    # Stable across credentials — the provider has no notion of identity.
    provider = AnonymousProvider()
    assert provider.key_id("anything") == "anon"
    assert provider.key_id("") == "anon"


def test_provider_name_is_anonymous() -> None:
    assert AnonymousProvider().name == "anonymous"


def test_constructor_absorbs_unused_kwargs() -> None:
    # Lifespan passes `config=` / `ckan=` for all providers uniformly;
    # the anonymous one must ignore them without error.
    AnonymousProvider(config=object(), ckan=object())
