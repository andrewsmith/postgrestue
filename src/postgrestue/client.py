"""A client that can create jobs in the postgrestue schema."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import socket
import sys
from typing import Optional
from uuid import UUID, uuid1

from prometheus_client import Counter, Histogram
import psycopg
from psycopg.rows import class_row

from .common import get_connection


logger = logging.getLogger(__name__)


jobs_enqueued = Counter(
    "postgrestue_jobs_enqueued",
    "Number of jobs enqueued by kind",
    ["kind"],
)

jobs_enqueue_times = Histogram(
    "postgrestue_jobs_enqueue_time_seconds",
    "Histogram of enqueue times for jobs",
)


@dataclass
class JobDescription:
    owner_id: UUID
    kind: str
    arguments: dict
    max_attempts: int
    timeout: timedelta
    schedule_time: Optional[datetime]
    blocking_job_id: Optional[UUID]
    queue: Optional[str]


def validate(job: JobDescription):
    if not job.kind:
        raise ValueError("kind is not specified")
    if job.max_attempts < 0:
        raise ValueError("max_attempts must be not be negative")
    if len([1 for f in [job.schedule_time, job.blocking_job_id, job.queue] if f]) > 1:
        raise ValueError("Only one of schedule_time, blocking_job_id, or queue may be set")


class Client:
    def __init__(self, conn: psycopg.AsyncConnection):
        self.conn = conn

    async def enqueue(self, job: JobDescription) -> UUID:
        validate(job)
        logger.info("Enqueuing %s", job)
        with jobs_enqueue_times.time():
            async with self.conn.transaction():
                async with self.conn.cursor() as cur:
                    job_id = uuid1()
                    create_time = datetime.now(timezone.utc)
                    await cur.execute(
                        """
                        INSERT INTO job (
                            id,
                            owner_id,
                            kind,
                            max_attempts,
                            timeout,
                            create_time,
                            schedule_time,
                            blocking_job_id,
                            queue,
                            arguments
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        params=(
                            job_id,
                            job.owner_id,
                            job.kind,
                            job.max_attempts,
                            job.timeout,
                            create_time,
                            job.schedule_time,
                            job.blocking_job_id,
                            job.queue,
                            json.dumps(job.arguments),
                        )
                    )
                    # Figure out what sort of bookkeeping steps need to be done to get the job running.
                    if job.schedule_time:
                        await cur.execute(
                            """
                            INSERT INTO scheduled_job (job_id, schedule_time) VALUES (%s, %s)
                            """,
                            params=(job_id, job.schedule_time)
                        )
                    elif job.blocking_job_id:
                        # See if the blocking job is currently running. Acquire a lock on it if
                        # it is. This prevents a race condition where the worker running it marks it
                        # as finished but doesn't see that this transaction is adding another
                        # blocked job on it.
                        await cur.execute(
                            """
                            SELECT 1
                            FROM running_job
                            WHERE job_id = %s
                            FOR SHARE
                            """,
                            params=(job.blocking_job_id,)
                        )
                        blocking_job_is_running = await cur.fetchone()
                        if not blocking_job_is_running:
                            # If the blocking job is not running, it may have already completed.
                            # If that is the case, this one can execute immediately.
                            await cur.execute(
                                """
                                SELECT 1
                                FROM finished_job
                                WHERE job_id = %s AND outcome = 'SUCCEEDED'
                                """,
                                params=(job_id,)
                            )
                            blocking_job_is_finished = await cur.fetchone()
                            if blocking_job_is_finished:
                                await cur.execute(
                                    """
                                    INSERT INTO running_job (job_id, attempt, state)
                                    VALUES (%s, 1, 'ENQUEUED')
                                    """,
                                    params=(job_id,)
                                )
                            else:
                                await cur.execute(
                                    """
                                    INSERT INTO blocked_job (job_id, blocking_job_id) VALUES (%s, %s)
                                    """,
                                    params=(job_id, job.blocking_job_id)
                                )
                        else:
                            # The blocking job is currently running. Record that it is blocked.
                            await cur.execute(
                                """
                                INSERT INTO blocked_job (job_id, blocking_job_id) VALUES (%s, %s)
                                """,
                                params=(job_id, job.blocking_job_id)
                            )
                    elif job.queue:
                        # Keep track of this job's position relative to others in the same queue by
                        # storing it in queued_job until it is finished.
                        await cur.execute(
                            """
                            INSERT INTO queued_job (queue, job_id) VALUES (%s, %s)
                            """,
                            params=(job.queue, job_id)
                        )
                        # Opportunistically try to put it in running_job. If there is another job
                        # from the same queue in there, this will do nothing.
                        await cur.execute(
                            """
                            INSERT INTO running_job (job_id, attempt, state, queue)
                            VALUES (%s, 1, 'ENQUEUED', %s)
                            ON CONFLICT (queue) DO NOTHING
                            """,
                            params=(job_id, job.queue)
                        )
                    else:
                        await cur.execute(
                            """
                            INSERT INTO running_job (job_id, attempt, state)
                            VALUES (%s, 1, 'ENQUEUED')
                            """,
                            params=(job_id,)
                        )
                    jobs_enqueued.labels(job.kind).inc()
                    return job_id


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    logger.info("Enqueuing a job...")
    owner_id = uuid1()

    async with await get_connection() as conn:
        client = Client(conn)

        job = JobDescription(
            owner_id,
            "send_welcome_email",
            {},
            3,
            timedelta(minutes=1),
            None,
            None,
            None,
        )
        await client.enqueue(job)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
