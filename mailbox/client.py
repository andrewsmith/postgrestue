"""A client that can create jobs in the mailbox schema."""

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
class JobDescription:
    owner_id: UUID
    kind: str
    arguments: dict
    max_attempts: int
    timeout: timedelta
    schedule_time: Optional[timedelta]
    blocking_job_id: Optional[UUID]
    queue: Optional[str]


def validate(job: JobDescription):
    if not job.kind:
        raise ValueError("kind is not specified")
    if job.max_attempts < 0:
        raise ValueError("max_attempts must be not be negative")
    if len([1 for f in [job.schedule_time, job.blocking_job_id, job.queue] if f]) > 1:
        raise ValueError("Only one of schedule_time, blocking_job_id, or queue may be set")


async def enqueue(conn: psycopg.AsyncConnection, job: JobDescription) -> UUID:
    validate(job)
    logger.info("Enqueuing %s", job)
    async with conn.transaction():
        async with conn.cursor() as cur:
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
                await cur.execute(
                    """
                    INSERT INTO blocked_job (job_id, blocking_job_id) VALUES (%s, %s)
                    """,
                    params=(job_id, job.blocking_job_id)
                )
            elif job.queue:
                # TODO(andrewsmith): Come up with a better implementation of this
                await cur.execute(
                    """SELECT max(position) || 0 FROM queued_job WHERE queue = %s""",
                    params=(job.queue)
                )
                last_position = await cur.fetchone()[0]
                position = last_position + 1
                await cur.execute(
                    """
                    INSERT INTO queued_job (queue, position, job_id) VALUES (%s, %s)
                    """,
                    params=(job.queue, position, job_id)
                )
                if position == 1:
                    # This is the first job in the queue, so start it running
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
                    INSERT INTO running_job (job_id, attempt, state)
                    VALUES (%s, 1, 'ENQUEUED')
                    """,
                    params=(job_id,)
                )
            return job_id


async def main(args):
    logging.basicConfig(level=logging.DEBUG)

    logger.info("Enqueuing a job...")
    owner_id = uuid1()

    async with await get_connection() as conn:
        job = JobDescription(
            owner_id,
            'SendWelcomeEmail',
            {},
            3,
            timedelta(minutes=1),
            None,
            None,
            None,
        )
        await enqueue(conn, job)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
