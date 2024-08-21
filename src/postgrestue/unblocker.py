"""A loop that advances jobs that have become unblocked.

When a blocked job is created, it doesn't know whether the blocking job has
finished already. I haven't found a way to do this at the time of enqueue at the
READ COMMITTED isolation level. This task is an inelegant way of keeping those
tasks moving forward.
"""

import asyncio
from datetime import datetime, timezone
import logging
import sys

import prometheus_client
from prometheus_client import Counter, Histogram

from .common import get_connection


logger = logging.getLogger(__name__)


unblocker_runs = Counter(
    "postgrestue_unblocker_runs",
    "How many times the unblocker has run",
)


unblocker_run_times = Histogram(
    "postgrestue_unblocker_run_time_seconds",
    "Histogram of how long the unblocker takes to run",
)


unblocker_jobs_processed = Counter(
    "postgrestue_unblocker_jobs_processed",
    "How many jobs have been processed so far",
)


class Unblocker:
    def __init__(self, conn):
        self.conn = conn

    async def process_unblocked_jobs(self):
        with unblocker_run_times.time():
            async with self.conn.transaction():
                async with self.conn.cursor() as cur:
                    await cur.execute(
                        """
                        DELETE FROM blocked_job b
                        USING finished_job f
                        WHERE b.blocking_job_id = f.job_id AND f.outcome = 'SUCCEEDED'
                        RETURNING b.job_id
                        """,
                    )
                    unblocked_jobs = await cur.fetchmany()
                    await cur.executemany(
                        """
                        INSERT INTO running_job (job_id, attempt, state) VALUES (%s, 1, 'ENQUEUED')
                        """,
                        params_seq=unblocked_jobs
                    )
                    logger.info("Unblocked %s jobs", len(unblocked_jobs))
                    unblocker_runs.inc()
                    unblocker_jobs_processed.inc(len(unblocked_jobs))

    async def run(self):
        logger.info("Starting unblocker...")
        while True:
            await self.process_unblocked_jobs()
            await asyncio.sleep(5)


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    prometheus_client.start_http_server(8003)
    logger.info("Prometheus metrics are available at http://localhost:8003/")

    async with await get_connection() as conn:
        await Unblocker(conn).run()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
