"""A 'front-end' web API to enqueue jobs and monitor them."""

import contextlib
from datetime import timedelta
from uuid import UUID, uuid1

import prometheus_client
from psycopg_pool import AsyncConnectionPool
import psycopg
from starlette.applications import Starlette
from starlette.config import Config
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette_prometheus import PrometheusMiddleware

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


async def place_order(request):
    body = await request.json()
    async with request.state.pool.connection() as conn:
        client = Client(conn)
        job = JobDescription(
            owner_id,
            "place_order",
            {"quantity": body["quantity"]},
            1,
            timedelta(minutes=1),
            None,
            None,
            body["sku"],
        )
        job_id = await client.enqueue(job)
        return JSONResponse({"job_id": str(job_id)})


async def message(request):
    body = await request.json()
    blocking_job_id = None
    if "parent_id" in body:
        blocking_job_id = UUID(body["parent_id"])
    async with request.state.pool.connection() as conn:
        client = Client(conn)
        job = JobDescription(
            owner_id,
            "message",
            {"message": body["message"]},
            1,
            timedelta(minutes=1),
            None,
            blocking_job_id,
            None,
        )
        job_id = await client.enqueue(job)
        return JSONResponse({"job_id": str(job_id)})


prometheus_metrics_app = prometheus_client.make_asgi_app()


app = Starlette(
    routes = [
        Route("/", index),
        Route("/health", health),
        Mount("/metrics", app=prometheus_metrics_app),
        Route("/register", register, methods=["POST"]),
        Route("/place_order", place_order, methods=["POST"]),
        Route("/message", message, methods=["POST"]),
    ],
    middleware = [
        Middleware(PrometheusMiddleware),
    ],
    lifespan=lifespan,
)
