"""Shared dependencies: two pools, two privilege levels.

Read routes get the SELECT-only pool. A bug in a read path cannot
delete anything, because Postgres refuses before the code is wrong.
"""
from __future__ import annotations

import asyncpg
from fastapi import Request

from src.db import create_ro_pool, create_rw_pool


class Pools:
    def __init__(self) -> None:
        self.rw: asyncpg.Pool | None = None
        self.ro: asyncpg.Pool | None = None

    async def open(self) -> None:
        self.rw = await create_rw_pool(min_size=1, max_size=4)
        self.ro = await create_ro_pool(min_size=1, max_size=4)

    async def close(self) -> None:
        if self.rw is not None:
            await self.rw.close()
        if self.ro is not None:
            await self.ro.close()


pools = Pools()


def get_rw(request: Request) -> asyncpg.Pool:
    return request.app.state.pools.rw


def get_ro(request: Request) -> asyncpg.Pool:
    return request.app.state.pools.ro