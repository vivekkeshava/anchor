"""Connection pool helpers."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

DEFAULT_DSN = "postgresql://anchor:anchor@localhost:55432/anchor"


def dsn() -> str:
    return os.environ.get("ANCHOR_DSN", DEFAULT_DSN)


async def create_pool(url: str | None = None, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(url or dsn(), min_size=min_size, max_size=max_size)


@asynccontextmanager
async def pool(url: str | None = None, **kwargs) -> AsyncIterator[asyncpg.Pool]:
    created = await create_pool(url, **kwargs)
    try:
        yield created
    finally:
        await created.close()
