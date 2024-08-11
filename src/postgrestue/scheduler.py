"""A scheduler that advances jobs that are ready to run."""

import asyncio
from datetime import datetime, timezone
import logging
import sys

import prometheus_client
from prometheus_client import Counter, Histogram

from .common import get_connection


logger = logging.getLogger(__name__)


scheduler_runs = Counter(
    "postgrestue_scheduler_runs",
    "How many times the scheduler has run",
)


scheduler_run_times = Histogram(
    "postgrestue_scheduler_run_time_seconds",
    "Histogram of how long the scheduler takes to run",
)


scheduler_jobs_processed = Counter(
    "postgrestue_scheduler_jobs_processed",
    "How many jobs have been processed so far",
)


class Scheduler:
    def __init__(self, conn):
        self.conn = conn

    async def process_ready_jobs(self):
        with scheduler_run_times.time():
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
                    scheduler_runs.inc()
                    scheduler_jobs_processed.inc(len(scheduled_jobs))

    async def run(self):
        logger.info("Starting scheduler...")
        while True:
            await self.process_ready_jobs()
            await asyncio.sleep(5)


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    prometheus_client.start_http_server(8002)
    logger.info("Prometheus metrics are available at http://localhost:8002/")

    async with await get_connection() as conn:
        await Scheduler(conn).run()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
