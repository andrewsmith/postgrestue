from datetime import timedelta
from uuid import uuid1

from postgrestue.client import Client, JobDescription


async def test_enqueue_simple_job(db):
    client = Client(db)

    owner_id = uuid1()
    description = JobDescription(
        owner_id,
        "SimpleJob",
        {},
        1,
        timedelta(minutes=1),
        None,
        None,
        None,
    )
    job_id = await client.enqueue(description)
    assert job_id
