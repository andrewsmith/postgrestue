"""An example implementation of a worker that uses the postgrestue schema."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import errno
import functools
import inspect
import json
import logging
import os
import socket
import sys
from typing import Optional
from uuid import UUID, uuid1

import prometheus_client
from prometheus_client import Counter, Histogram
import psycopg
from psycopg.rows import class_row

from .common import get_connection, retry_serialization_failures


logger = logging.getLogger(__name__)


jobs_dequeue_times = Histogram(
    "postgrestue_jobs_dequeue_time_seconds",
    "Histogram of dequeue times for jobs",
    buckets=[0.0001,0.0005,0.001,0.005,0.01,0.02,0.05,0.1,0.2,0.5],
)

jobs_invocation_times = Histogram(
    "postgrestue_jobs_invocation_time_seconds",
    "Histogram of how long jobs take to run",
)

jobs_dequeued = Counter(
    "postgrestue_jobs_dequeued",
    "Number of jobs dequeued by kind",
    ["kind"],
)

jobs_succeeded = Counter(
    "postgrestue_jobs_succeeded",
    "Number of jobs that suceeded by kind",
    ["kind"],
)

jobs_failed = Counter(
    "postgrestue_jobs_failed",
    "Number of jobs that failed by kind",
    ["kind"],
)

jobs_retried = Counter(
    "postgrestue_jobs_retried",
    "Number of jobs that are to be retried by kind",
    ["kind"],
)

jobs_unblocked = Counter(
    "postgrestue_jobs_unblocked",
    "Number of jobs unblocked",
)

jobs_unfifoed = Counter(
    "postgrestue_jobs_unfifoed",
    "Number of jobs moved from a FIFO queue to be run",
    ["queue"],
)


def now():
    return datetime.now(timezone.utc)


@dataclass
class Job:
    """A job that is currently being processed."""

    id: UUID
    kind: str
    arguments: dict
    attempt: int
    max_attempts: int
    timeout: timedelta
    blocking_job_id: Optional[UUID]
    queue: str
    start_time: datetime = field(default_factory=now)
    finish_time: Optional[datetime] = None

    @property
    def can_retry(self):
        return self.attempt < self.max_attempts

    def mark_finished(self):
        self.finish_time = now()


class BookkeepingStats:
    """Track how many jobs were updated as part of finishing this one."""
    jobs_unblocked: int = 0
    jobs_unfifoed: int = 0


class Worker:
    def __init__(self, conn, functions, name=None, worker_id=None, executor=None):
        self.conn = conn
        self.functions = functions
        self.worker_id = worker_id or uuid1()
        self.name = name or socket.getfqdn()
        self.executor = executor
        self.ping_frequency = timedelta(seconds=5)
        self.last_ping = None

    async def send_ping(self):
        ping_now = now()
        if not self.last_ping or self.last_ping + self.ping_frequency <= ping_now:
            async with self.conn.cursor() as cur:
                logger.debug("Sending an activity ping")
                await cur.execute(
                    """
                    UPDATE worker SET last_ping_time = %s WHERE id = %s
                    """,
                    params=(ping_now, self.worker_id)
                )
            self.last_ping = ping_now

    async def process_one_job(self):
        logger.debug("Looking for a job to process")
        await self.send_ping()
        with jobs_dequeue_times.time():
            job = await self._dequeue()
            if not job:
                logger.debug("No available jobs found")
                return False
        jobs_dequeued.labels(job.kind).inc()
        try:
            logger.info("Invoking %s", job)
            async with asyncio.timeout(job.timeout.total_seconds()):
                await self._invoke(job)
            job.mark_finished()
        except Exception as e:
            job.mark_finished()
            logger.exception("Job failed: #{e}")
            await self._report_failure_and_maybe_retry(job)

            # Update counters
            jobs_failed.labels(job.kind).inc()
            if job.can_retry:
                jobs_retried.labels(job.kind).inc()
        else:
            logger.info("Finished job %s", job.id)
            stats = await self._report_success_and_do_bookkeeping(job)

            # Update counters
            jobs_succeeded.labels(job.kind).inc()
            jobs_unblocked.inc(stats.jobs_unblocked)
            if stats.jobs_unfifoed:
                jobs_unfifoed.labels(job.queue).inc(stats.jobs_unfifoed)
        return True

    @retry_serialization_failures
    async def _dequeue(self) -> Job:
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
                    return None

                # Mark the job as being processed by this worker
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
                    params=(job.start_time, self.worker_id, job.id)
                )
                return job

    async def _invoke(self, job):
        try:
            function = getattr(self.functions, job.kind)
        except AttributeError:
            function = self.functions[job.kind]

        with jobs_invocation_times.time():
            if inspect.iscoroutinefunction(function):
                await function(**job.arguments)
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    self.executor,
                    functools.partial(function, **job.arguments),
                )


    @retry_serialization_failures
    async def _report_failure_and_maybe_retry(self, job):
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
                    params=(job.id, job.attempt, job.start_time, job.finish_time)
                )
                if job.can_retry:
                    await cur.execute(
                        """
                        INSERT INTO running_job (job_id, attempt, state, queue)
                        VALUES (%s, %s, 'ENQUEUED', %s)
                        """,
                        params=(job.id, job.attempt + 1, job.queue)
                    )

    @retry_serialization_failures
    async def _report_success_and_do_bookkeeping(self, job) -> BookkeepingStats:
        stats = BookkeepingStats()
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
                    params=(job.id, job.attempt, job.start_time, job.finish_time)
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
                    stats.jobs_unblocked = len(blocked_job_ids)
                #   If the job came from a queue, see if there is a next job to pull into running_job
                if job.queue:
                    await cur.execute(
                        """
                        DELETE FROM queued_job WHERE job_id = %s RETURNING position
                        """,
                        params=(job.id,)
                    )
                    position = (await cur.fetchone())[0]
                    # See if there is another job in the queue and start it
                    await cur.execute(
                        """
                        SELECT job_id
                        FROM queued_job
                        WHERE queue = %s AND position > %s
                        ORDER BY position ASC
                        LIMIT 1
                        """,
                        params=(job.queue, position)
                    )
                    row = await cur.fetchone()
                    if row:
                        job_id = row[0]
                        await cur.execute(
                            """
                            INSERT INTO running_job (job_id, attempt, state, queue)
                            VALUES (%s, 1, 'ENQUEUED', %s)
                            """,
                            params=(job_id, job.queue)
                        )
                        stats.jobs_unfifoed = 1
        return stats

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
                params=(self.worker_id, self.name)
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
        logger.info("Starting worker %s (%s)...", self.name, self.worker_id)
        await self.register()
        try:
            await self.process_jobs()
        finally:
            await self.deregister()


async def main(args):
    import time

    def send_welcome_email(**kwargs):
        logger.info("Calling send_welcome_email")

    def place_order(*, sku=None, quantity=None):
        logger.info("Placed an order for %s %s", quantity, sku)

    def message(*, message=None):
        logger.info("Message is: %s", message)

    def sleep(*, seconds=None):
        logger.info("Sleeping for %s seconds", seconds)
        time.sleep(seconds)
        logger.info("Slept for %s seconds", seconds)

    sample_functions = dict(
        send_welcome_email=send_welcome_email,
        place_order=place_order,
        message=message,
        sleep=sleep,
    )

    logging.basicConfig(level=logging.DEBUG)

    # For Prometheus, try ascending port numbers until one is available.
    port = 8005
    while True:
        try:
            prometheus_client.start_http_server(port)
            logger.info("Prometheus metrics are available at http://localhost:%s/", port)
        except OSError as e:
            if e.errno == errno.EADDRINUSE:
                port += 1
            else:
                raise
        else:
            break

    hostname = socket.getfqdn()
    pid = os.getpid()

    workers = 4
    executor = ThreadPoolExecutor(max_workers=workers)

    async with asyncio.TaskGroup() as group:
        for i in range(workers):
            conn = await get_connection()
            worker = Worker(
                conn,
                sample_functions,
                name=f"{hostname}-{pid}-{i}",
                executor=executor,
            )
            group.create_task(worker.run(), name=f"worker-{i}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
