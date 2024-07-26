"""A scheduler that advances jobs that are ready to run."""

import asyncio
from datetime import datetime, timezone
import logging
import os
import sys

import psycopg


logger = logging.getLogger(__name__)


async def move_scheduled_jobs_to_running(conn):
    while True:
        now = datetime.now(timezone.utc)
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM scheduled_job WHERE schedule_time <= %s RETURNING job_id
                    """,
                    params=(now,)
                )
                scheduled_jobs = await cur.fetchmany()
                await cur.executemany(
                    """
                    INSERT INTO running_job (job_id, attempt, state) VALUES (%s, 1, 'ENQUEUED')
                    """,
                    params_seq=scheduled_jobs
                )
                logger.info("Marked %s jobs as ready to run", len(scheduled_jobs))
        await asyncio.sleep(5)


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    logger.info("Starting scheduler...")

    # Set up a connection
    try:
        database_url = os.environ["DATABASE_URL"]
    except KeyError:
        raise RuntimeError("DATABASE_URL is not set")

    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        await move_scheduled_jobs_to_running(conn)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
