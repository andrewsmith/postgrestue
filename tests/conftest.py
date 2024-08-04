"""Supporting code for tests."""

from pathlib import Path

import psycopg
import pytest_asyncio
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor


postgrestue_postgresql_proc = factories.postgresql_proc(
    dbname="postgrestue",
    load=[
        (Path(__file__).parent.parent / "schema.sql")
    ]
)


@pytest_asyncio.fixture
async def db(postgrestue_postgresql_proc):
    """A fixture that provides an async connection for tests to use.

    This also sets up the database structure from the project's schema.sql file.
    """
    executor = postgrestue_postgresql_proc
    with DatabaseJanitor(
        host=executor.host,
        port=executor.port,
        user=executor.user,
        dbname=executor.dbname,
        template_dbname=executor.template_dbname,
        password=executor.password,
        version=executor.version,
    ):
        async with await psycopg.AsyncConnection.connect(
            host=executor.host,
            port=executor.port,
            user=executor.user,
            dbname=executor.dbname,
            password=executor.password,
            options=executor.options,
        ) as conn:
            yield conn
