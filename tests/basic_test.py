from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid1

from postgrestue.client import Client, JobDescription
from postgrestue.worker import Worker
from postgrestue.scheduler import Scheduler


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


async def test_scheduled_job(db):
    client = Client(db)

    owner_id = uuid1()
    arguments = dict(name="Andrew")
    description = JobDescription(
        owner_id,
        "say_hello",
        arguments,
        1,
        timedelta(minutes=1),
        datetime.now(timezone.utc),
        None,
        None,
    )
    job_id = await client.enqueue(description)
    assert job_id

    # Run the worker with a mock
    say_hello_mock = Mock()
    functions = dict(say_hello=say_hello_mock)
    worker = Worker(db, functions)

    # No job should be found initially
    found_job = await worker.process_one_job()
    assert not found_job

    # Now run the scheduler
    await Scheduler(db).process_ready_jobs()

    # Now try to run the worker and expect it to find (and run) the job
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


async def test_blocked_job(db):
    client = Client(db)

    owner_id = uuid1()
    arguments1 = dict(name="Andrew")
    description1 = JobDescription(
        owner_id,
        "say_hello",
        arguments1,
        1,
        timedelta(minutes=1),
        None,
        None,
        None,
    )
    job_id1 = await client.enqueue(description1)
    assert job_id1

    arguments2 = dict(name="Nick")
    description2 = JobDescription(
        owner_id,
        "say_hello",
        arguments2,
        1,
        timedelta(minutes=1),
        None,
        job_id1,  # Job 2 waits for Job 1
        None,
    )
    job_id2 = await client.enqueue(description2)
    assert job_id2

    # Run the worker with a mock
    say_hello_mock = Mock()
    functions = dict(say_hello=say_hello_mock)
    worker = Worker(db, functions)

    # See that the first job is run correctly
    found_job = await worker.process_one_job()
    assert found_job
    say_hello_mock.assert_called_once_with(**arguments1)
    say_hello_mock.reset_mock()

    # See that the second job is run correctly
    found_job = await worker.process_one_job()
    assert found_job
    say_hello_mock.assert_called_once_with(**arguments2)
