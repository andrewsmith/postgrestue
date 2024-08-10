"""A 'front-end' web API to enqueue jobs and monitor them."""

import contextlib
from datetime import timedelta
from uuid import uuid1

from psycopg_pool import AsyncConnectionPool
import psycopg
from starlette.applications import Starlette
from starlette.config import Config
from starlette.responses import JSONResponse
from starlette.routing import Route

from postgrestue.client import Client, JobDescription


config = Config()


DATABASE_URL = config("DATABASE_URL")


owner_id = uuid1()


@contextlib.asynccontextmanager
async def lifespan(app):
    async with AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10) as pool:
        yield {"pool": pool}


async def index(request):
    return JSONResponse({"message": "Welcome to Postgrestue!"})


async def health(request):
    async with request.state.pool.connection() as conn:
        connected = conn.info.status == psycopg.pq.ConnStatus.OK
        return JSONResponse({"db": {"connected": connected}})


async def register(request):
    body = await request.json()
    async with request.state.pool.connection() as conn:
        client = Client(conn)
        job = JobDescription(
            owner_id,
            "send_welcome_email",
            {"name": body["name"]},
            1,
            timedelta(minutes=1),
            None,
            None,
            None,
        )
        job_id = await client.enqueue(job)
        return JSONResponse({"job_id": str(job_id)})


app = Starlette(
    routes = [
        Route("/", index),
        Route("/health", health),
        Route("/register", register, methods=["POST"])
    ],
    lifespan=lifespan,
)
