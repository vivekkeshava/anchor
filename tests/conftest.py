from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

from anchor.db import DEFAULT_DSN
from anchor.journal.repo import JournalRepo

DSN = os.environ.get("ANCHOR_TEST_DSN", DEFAULT_DSN)


@pytest_asyncio.fixture
async def pool():
    try:
        created = await asyncpg.create_pool(DSN, min_size=1, max_size=8)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no Postgres at {DSN}: {exc}")
    try:
        yield created
    finally:
        await created.close()


@pytest_asyncio.fixture
async def repo(pool):
    repo = JournalRepo(pool)
    await repo.create_schema()
    # Every test starts from empty: these tests assert on global counts (ledger rows, model
    # calls), which leftover rows would quietly corrupt.
    await repo.truncate_all()
    return repo
