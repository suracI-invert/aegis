import logging

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

logger = logging.getLogger(__name__)


async def check_schema(engine: sa_async.AsyncEngine):
    """Check if the database schema is up to date."""
    async with engine.begin() as conn:
        await conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS 'aegis'"))

        table = (
            await conn.execute(
                sa.text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'aegis' AND table_name = 'aegis_migrations'"
                ),
            )
        ).fetchone() is not None
        if not table:
            logger.info("Creating aegis_migrations table")
            await conn.execute(
                sa.text(
                    """CREATE TABLE aegis.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"""
                )
            )


async def run_migrations(engine: sa_async.AsyncEngine):
    """Run pending migrations."""
    async with engine.begin() as conn:
        ...
