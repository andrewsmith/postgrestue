from contextlib import contextmanager
import logging
import os

from prometheus_client import Counter, Histogram
import psycopg


logger = logging.getLogger(__name__)


serialization_failures = Counter(
    "postgrestue_serialization_failures",
    "Number of serialization failures encountered that need to be retried",
)


async def get_connection():
    try:
        database_url = os.environ["DATABASE_URL"]
    except KeyError:
        raise RuntimeError("DATABASE_URL is not set")
    return await psycopg.AsyncConnection.connect(database_url, autocommit=True)


def retry_serialization_failures(f):
    async def _wrapper(*args, **kwargs):
        while True:
            try:
                return await f(*args, **kwargs)
            except psycopg.errors.SerializationFailure:
                logger.warning("Retrying due to a serialization failure")
                serialization_failures.inc()

    return _wrapper
