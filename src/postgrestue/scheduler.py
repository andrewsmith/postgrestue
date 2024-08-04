"""A scheduler that advances jobs that are ready to run."""

import asyncio
from datetime import datetime, timezone
import logging
import sys

from .common import get_connection


logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, conn):
        self.conn = conn

    async def run(self):
        logger.info("Starting scheduler...")
        while True:
            now = datetime.now(timezone.utc)
            async with self.conn.transaction():
                async with self.conn.cursor() as cur:
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

    async with await get_connection() as conn:
        await Scheduler(conn).run()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
