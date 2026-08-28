"""asyncpg connection pools for Supabase's transaction pooler.

Two pools, two privilege levels:
  rw_pool() -> the processor writes through this
  ro_pool() -> read endpoints read through this (SELECT only)
"""
from __future__ import annotations

import asyncpg

from src.config import DB_URL_RO, DB_URL_RW

# Supabase's transaction pooler (port 6543) assigns a different backend
# connection per query, so asyncpg's named prepared-statement cache
# collides across them. Disabling the cache is mandatory here.
_POOLER_SAFE = {
    "statement_cache_size": 0,
    "max_cacheable_statement_size": 0,
}


async def create_rw_pool(
    min_size: int = 1, max_size: int = 5
) -> asyncpg.Pool:
    """Pool for the `postgres` role. Reads and writes."""
    return await asyncpg.create_pool(
        dsn=DB_URL_RW,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
        server_settings={"application_name": "f1stream-rw"},
        **_POOLER_SAFE,
    )


async def create_ro_pool(
    min_size: int = 1, max_size: int = 5
) -> asyncpg.Pool:
    """Pool for the `stream_readonly` role. SELECT only, enforced by
    Postgres privileges — not by convention in application code."""
    return await asyncpg.create_pool(
        dsn=DB_URL_RO,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
        server_settings={"application_name": "f1stream-ro"},
        **_POOLER_SAFE,
    )