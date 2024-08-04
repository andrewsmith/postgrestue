"""An example implementation of a worker that uses the postgrestue schema."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import socket
import sys
from typing import Optional
from uuid import UUID, uuid1

import psycopg
from psycopg.rows import class_row

from .common import get_connection


logger = logging.getLogger(__name__)


@dataclass
class Job:
    """Represents a job that is currently being processed."""

    id: UUID
    kind: str
    arguments: dict
    attempt: int
    max_attempts: int
    timeout: timedelta
    blocking_job_id: Optional[UUID]
    queue: str


async def duplicate_connection(conn: psycopg.AsyncConnection) -> psycopg.AsyncConnection:
    """Create a new connection based on an open one."""
    return await psycopg.AsyncConnection.connect(
        conn.info.dsn,
        password=conn.info.password,
        autocommit=conn.autocommit,
        row_factory=conn.row_factory,
        cursor_factory=conn.cursor_factory,
        prepare_threshold=conn.prepare_threshold,
    )


class Worker:
    def __init__(self, conn, functions, hostname=None, worker_id=None):
        self.conn = conn
        self.functions = functions
        self.worker_id = worker_id or uuid1()
        self.hostname = hostname or socket.getfqdn()
        self.ping_frequency_seconds = 5
        self.ping_activity = False

    async def send_periodic_pings(self):
        async with await duplicate_connection(self.conn) as conn:
            while True:
                if self.ping_activity:
                    self.ping_activity = False
                    async with conn.cursor() as cur:
                        now = datetime.now(timezone.utc)
                        logger.debug("Sending an activity ping")
                        await cur.execute(
                            """
                            UPDATE worker SET last_ping_time = %s WHERE id = %s
                            """,
                            params=(now, self.worker_id)
                        )
                await asyncio.sleep(self.ping_frequency_seconds)

    async def process_one_job(self):
        logger.debug("Looking for a job to process")
        self.ping_activity = True
        async with self.conn.transaction():
            async with self.conn.cursor(row_factory=class_row(Job)) as cur:
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
                    """
                )
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
                    params=(start_time, self.worker_id, job.id)
                )
        try:
            logger.info("Invoking %s", job)
            self._invoke(job)
            finish_time = datetime.now(timezone.utc)
        except Exception as e:
            finish_time = datetime.now(timezone.utc)
            logger.exception("Job failed: #{e}")
            async with self.conn.transaction():
                async with self.conn.cursor() as cur:
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
                            params=(job.id, next_attempt)
                        )
        else:
            async with self.conn.transaction():
                async with self.conn.cursor() as cur:
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

    def _invoke(self, job):
        try:
            function = getattr(self.functions, job.kind)
        except AttributeError:
            function = self.functions[job.kind]
        function(**job.arguments)

    async def process_jobs(self):
        # Loop through processing jobs
        while True:
            for delay in [0.25, 0.5, 1, 2, 4, 8, 16, 32]:
                did_dequeue_job = await self.process_one_job()
                if did_dequeue_job:
                    break
                else:
                    await asyncio.sleep(delay)

    async def register(self):
        logger.debug("Registering")
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO worker (id, name, register_time, last_ping_time)
                VALUES (
                    %s, %s, current_timestamp, current_timestamp
                )
                """,
                params=(self.worker_id, self.hostname)
            )

    async def deregister(self):
        logger.debug("De-registering")
        async with self.conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM worker WHERE id = %s
                """,
                params=(self.worker_id,)
            )

    async def run(self):
        logger.info("Starting worker %s on %s...", self.worker_id, self.hostname)
        await self.register()
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self.send_periodic_pings())
                group.create_task(self.process_jobs())
        finally:
            await self.deregister()


async def main(args):
    def send_welcome_email(**kwargs):
        logger.info("Calling send_welcome_email")

    sample_functions = dict(send_welcome_email=send_welcome_email)

    logging.basicConfig(level=logging.DEBUG)
    async with await get_connection() as conn:
        await Worker(conn, sample_functions).run()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
