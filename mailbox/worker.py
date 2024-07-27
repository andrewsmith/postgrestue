"""An example implementation of a worker that uses the mailbox schema."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import socket
import sys
from typing import Optional
from uuid import UUID, uuid1

from psycopg.rows import class_row

from .common import get_connection


logger = logging.getLogger(__name__)


PING_FREQUENCY_SECONDS = 5
ping_activity = False


def mark_activity():
    global ping_activity
    ping_activity = True


async def send_periodic_pings(worker_id):
    global ping_activity
    async with await get_connection() as conn:
        while True:
            if ping_activity:
                ping_activity = False
                async with conn.cursor() as cur:
                    now = datetime.now(timezone.utc)
                    logger.debug("Sending an activity ping")
                    await cur.execute(
                        """
                        UPDATE worker SET last_ping_time = %s WHERE id = %s
                        """,
                        params=(now, worker_id)
                    )
            await asyncio.sleep(PING_FREQUENCY_SECONDS)


@dataclass
class Job:
    id: UUID
    kind: str
    arguments: dict
    attempt: int
    max_attempts: int
    timeout: timedelta
    blocking_job_id: Optional[UUID]
    queue: str


async def process_one_job(conn, worker_id):
    logger.debug("Looking for a job to process")
    mark_activity()
    async with conn.transaction():
        async with conn.cursor(row_factory=class_row(Job)) as cur:
            # Select a job to process
            await cur.execute(
                """
                SELECT
                  j.id,
                  j.kind,
                  j.arguments,
                  rj.attempt,
                  j.max_attempts,
                  j.timeout,
                  j.blocking_job_id,
                  j.queue
                FROM
                  running_job rj
                    JOIN job j ON (rj.job_id = j.id)
                WHERE
                  rj.state = 'ENQUEUED'
                LIMIT 1
                FOR UPDATE OF rj SKIP LOCKED
                """)
            job = await cur.fetchone()
            if not job:
                logger.debug("No available jobs found")
                return False

            # Mark the job as being processed by this worker
            start_time = datetime.now(timezone.utc)
            await cur.execute(
                """
                UPDATE running_job
                SET
                  start_time = %s,
                  state = 'PROCESSING',
                  worker_id = %s
                WHERE
                  job_id = %s
                """,
                params=(start_time, worker_id, job.id)
            )
    try:
        # Invoke it
        logger.info("Invoking %s", job)
        finish_time = datetime.now(timezone.utc)
    except Exception as e:
        finish_time = datetime.now(timezone.utc)
        logger.execption("Job failed: #{e}")
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM running_job WHERE job_id = %s
                    """,
                    params=(job.id,)
                )
                await cur.execute(
                    """
                    INSERT INTO finished_job (job_id, attempt, outcome, start_time, finish_time, result)
                    VALUES (%s, %s, 'FAILED', %s, %s, NULL)
                    """,
                    params=(job.id, job.attempt, start_time, finish_time)
                )

                # Create a new attempt if not exceeding max_attempts
                next_attempt = job.attempt + 1
                if next_attempt <= job.max_attempts:
                    await cur.execute(
                        """
                        INSERT INTO running_job (job_id, attempt, state)
                        VALUES (%s, %s, 'ENQUEUED')
                        """,
                        params=(job_id, next_attempt)
                    )
    else:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM running_job WHERE job_id = %s
                    """,
                    params=(job.id,)
                )
                await cur.execute(
                    """
                    INSERT INTO finished_job (job_id, attempt, outcome, start_time, finish_time, result)
                    VALUES (%s, %s, 'SUCCEEDED', %s, %s, NULL)
                    """,
                    params=(job.id, job.attempt, start_time, finish_time)
                )
                # Do any bookkeeping necessary
                #   If the job is blocking anything else, move those to running_job
                await cur.execute(
                    """
                    DELETE FROM blocked_job WHERE blocking_job_id = %s RETURNING job_id
                    """,
                    params=(job.id,)
                )
                blocked_job_ids = await cur.fetchmany()
                if blocked_job_ids:
                    await cur.executemany(
                        """
                        INSERT INTO running_job (job_id, attempt, state)
                        VALUES (%s, 1, 'ENQUEUED')
                        """,
                        params_seq=blocked_job_ids
                    )
                #   If the job came from a queue, see if there is a next job to pull into running_job
                if job.queue:
                    await cur.execute(
                        """
                        DELETE FROM queued_job WHERE job_id = %s RETURNING position
                        """
                    )
                    position = cur.fetchone()[0]
                    # See if there is another job in the queue and start it
                    await cur.execute(
                        """
                        SELECT job_id FROM queued_job WHERE queue = %s AND position = %s
                        """,
                        params=(job.queue, position + 1)
                    )
                    row = await cur.fetchone()
                    if row:
                        job_id = row[0]
                        await cur.execute(
                            """
                            INSERT INTO running_job (job_id, attempt, state)
                            VALUES (%s, 1, 'ENQUEUED')
                            """,
                            params=(job_id,)
                        )
        logger.info("Finished job %s", job.id)
    return True


async def register_worker(conn, worker_id, hostname):
    logger.debug("Registering")
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO worker (id, name, register_time, last_ping_time)
            VALUES (
                %s, %s, current_timestamp, current_timestamp
            )
            """,
            params=(worker_id, hostname)
        )


async def deregister_worker(conn, worker_id):
    logger.debug("De-registering")
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM worker WHERE id = %s
            """,
            params=(worker_id,)
        )


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    worker_id = uuid1()
    hostname = socket.getfqdn()
    logger.info("Starting worker %s on %s...", worker_id, hostname)


    # Set up a connection
    async with await get_connection() as conn:
        await register_worker(conn, worker_id, hostname)

        ping_task = asyncio.create_task(send_periodic_pings(worker_id))

        # Loop through processing jobs
        while True:
            for delay in [0.25, 0.5, 1, 2, 4, 8, 16, 32]:
                did_dequeue_job = await process_one_job(conn, worker_id)
                if did_dequeue_job:
                    break
                else:
                    await asyncio.sleep(delay)

        await ping_task
        await deregister_worker(conn, worker_id)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
