"""An example implementation of a worker that uses the mailbox schema."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
import socket
import sys
from typing import Optional
from uuid import UUID, uuid1

import psycopg
from psycopg.rows import class_row


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
    print("Looking for a job to process")
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
                print("No available jobs found")
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
        print(f"Invoking {job}")
        finish_time = datetime.now(timezone.utc)
    except:
        print("Job failed")
        # Create a new attempt if not exceeding max_attempts
        # Insert this one into finished_job
        pass
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
                #   If the job came from a queue, see if there is a next job to pull into running_job
        print("Finished job")


async def move_scheduled_jobs_to_running(conn):
    while True:
        # Select all jobs that have a scheduled start time earlier than now
        # Move them in batches to running_job
        pass


async def register_worker(conn, worker_id, hostname):
    print("Registering")
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
    print("De-registering")
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM worker WHERE id = %s
            """,
            params=(worker_id,)
        )


async def main(args):
    print("Starting worker...")
    worker_id = uuid1()
    hostname = socket.getfqdn()

    # Set up a connection
    try:
        database_url = os.environ["DATABASE_URL"]
    except KeyError:
        raise RuntimeError("DATABASE_URL is not set")
    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        await register_worker(conn, worker_id, hostname)

        # Start move_scheduled_jobs_to_running
        # Loop through processing jobs
        await process_one_job(conn, worker_id)

        await deregister_worker(conn, worker_id)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
