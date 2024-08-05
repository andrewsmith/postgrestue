from datetime import timedelta
from unittest.mock import Mock
from uuid import uuid1

from postgrestue.client import Client, JobDescription
from postgrestue.worker import Worker


async def test_process_simple_job(db):
    client = Client(db)

    owner_id = uuid1()
    arguments = dict(name="Andrew")
    description = JobDescription(
        owner_id,
        "say_hello",
        arguments,
        1,
        timedelta(minutes=1),
        None,
        None,
        None,
    )
    job_id = await client.enqueue(description)
    assert job_id

    # Run the worker with a mock
    say_hello_mock = Mock()
    functions = dict(say_hello=say_hello_mock)
    worker = Worker(db, functions)
    found_job = await worker.process_one_job()
    assert found_job

    # Ensure that the mock function gets called correctly
    say_hello_mock.assert_called_once_with(**arguments)

    # Expect a finished job record in the DB
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT attempt, outcome, result
            FROM finished_job
            WHERE job_id = %s
            """,
            params=(job_id,)
        )
        results = await cur.fetchall()
        assert results == [(1, "SUCCEEDED", None)]

    # Attempting to process another job results in no jobs being found
    found_job = await worker.process_one_job()
    assert not found_job
