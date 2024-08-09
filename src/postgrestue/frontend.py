"""A 'front-end' web API to enqueue jobs and monitor them."""

import contextlib

from psycopg_pool import AsyncConnectionPool
import psycopg
from starlette.applications import Starlette
from starlette.config import Config
from starlette.responses import JSONResponse
from starlette.routing import Route

from postgrestue.client import Client


config = Config()


DATABASE_URL = config("DATABASE_URL")


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


app = Starlette(
    routes = [
        Route("/", index),
        Route("/health", health)
    ],
    lifespan=lifespan,
)
