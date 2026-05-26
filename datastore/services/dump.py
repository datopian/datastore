"""Service for multi-shard streaming of `/datastore/dump/<rid>`.

Single-shard exports are served as 302 redirects (bytes flow GCS →
client, never through us). When BigQuery shards an export (>1 GB
CSV/NDJSON), the endpoint falls back to **server-side stream-concat
through this module**: we fetch each shard from GCS via async httpx
and forward bytes to the client as a single download.

Resource profile per active stream-concat dump:
  - Memory: one ~64 KiB chunk in flight per active shard (we walk
    shards serially, so peak ≈ one chunk).
  - CPU: byte forwarding plus a single newline scan per CSV shard to
    strip its header. Essentially zero.
  - Threads: none — everything runs on the asyncio loop via httpx.
  - Network: full dump size through our server (the unavoidable cost
    of the "one URL → one file" contract).

Parquet shards aren't supported here because their footers can't be
byte-concat'd; the engine refuses multi-shard Parquet at 1 GB.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx

# Read-side network knobs. No total timeout (a multi-GB stream legitimately
# takes minutes); just a generous per-chunk read timeout so a dead GCS
# connection doesn't hang us forever.
_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
# Chunk size we pull from GCS / forward to the client.
_CHUNK_BYTES = 64 * 1024


async def stream_csv_shards(urls: list[str]) -> AsyncIterator[bytes]:
    """Stream-concat CSV shards. Header from the first shard only; the
    first newline of each subsequent shard is dropped (BigQuery emits
    a header row per shard when `header=true`)."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for i, url in enumerate(urls):
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                if i == 0:
                    async for chunk in resp.aiter_bytes(_CHUNK_BYTES):
                        yield chunk
                else:
                    async for chunk in _skip_first_line(
                        resp.aiter_bytes(_CHUNK_BYTES)
                    ):
                        yield chunk


async def stream_ndjson_shards(urls: list[str]) -> AsyncIterator[bytes]:
    """Stream-concat NDJSON shards. Each shard is independent
    newline-delimited JSON, so pure byte concatenation produces a
    valid combined stream."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for url in urls:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(_CHUNK_BYTES):
                    yield chunk


async def _skip_first_line(
    chunks: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    """Drop bytes up to and including the first `\\n`, then forward
    the rest unchanged. Used to strip the duplicate CSV header on
    non-first shards. Memory bound: bytes of the header line plus
    one chunk."""
    pending = bytearray()
    async for chunk in chunks:
        pending.extend(chunk)
        idx = pending.find(b"\n")
        if idx >= 0:
            yield bytes(pending[idx + 1:])
            pending.clear()
            break
    async for chunk in chunks:
        yield chunk


