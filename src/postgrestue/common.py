import os

import psycopg


async def get_connection():
    try:
        database_url = os.environ["DATABASE_URL"]
    except KeyError:
        raise RuntimeError("DATABASE_URL is not set")
    return await psycopg.AsyncConnection.connect(database_url, autocommit=True)
