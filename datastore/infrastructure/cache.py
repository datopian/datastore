from __future__ import annotations

import time
from typing import Protocol

import redis.asyncio as redis


class CachePort(Protocol):
    async def get(self, key: str) -> bytes | None: ...
    async def set(self, key: str, value: bytes, ttl: int) -> None: ...


class InMemoryCache:
    """In-process cache with per-key TTL.
    Used for tests, local dev, and as the fallback when Redis is unavailable.
    Not safe across processes — replace with `RedisCache` in production.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[float, bytes]] = {}

    async def get(self, key: str) -> bytes | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at < time.monotonic():
            self._data.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        self._data[key] = (time.monotonic() + ttl, value)


class RedisCache:
    """Async Redis cache adapter for production use."""

    def __init__(self, url: str) -> None:
        self._client = redis.from_url(url, decode_responses=False)

    async def get(self, key: str) -> bytes | None:
        value = await self._client.get(key)
        if value is None:
            return None
        assert isinstance(value, bytes)
        return value

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        await self._client.set(key, value, ex=ttl)

    async def close(self) -> None:
        await self._client.close()
