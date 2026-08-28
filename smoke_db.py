"""Step 4 smoke test: prove both pools connect and that the read-only
role genuinely cannot write."""
from __future__ import annotations

import asyncio

import asyncpg

from src.db import create_ro_pool, create_rw_pool


async def main() -> None:
    rw = await create_rw_pool()
    async with rw.acquire() as conn:
        version = await conn.fetchval("select version()")
        who = await conn.fetchval("select current_user")
        tables = await conn.fetchval(
            "select count(*) from information_schema.tables "
            "where table_schema = 'public'"
        )
    print(f"RW connected as {who}")
    print(f"   server: {version.split(',')[0]}")
    print(f"   public tables: {tables}")
    await rw.close()

    ro = await create_ro_pool()
    async with ro.acquire() as conn:
        who = await conn.fetchval("select current_user")
        count = await conn.fetchval("select count(*) from datasets")
        print(f"RO connected as {who}")
        print(f"   datasets readable: {count} rows")

        try:
            await conn.execute(
                "insert into datasets (label, kind, checksum) "
                "values ($1, $2, $3)",
                "should-fail",
                "synthetic",
                "smoke-test",
            )
        except asyncpg.InsufficientPrivilegeError:
            print("RO write correctly refused: insufficient privilege")
        else:
            print("SECURITY FAILURE: read-only role wrote a row. Stop.")
    await ro.close()


if __name__ == "__main__":
    asyncio.run(main())