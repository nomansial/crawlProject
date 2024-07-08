from typing import AsyncGenerator

import aioodbc
import psycopg_pool
from fastapi import Depends, Query

from config import settings
from repository.db_repository import QueueRepository

# from job_postings_crawler.config import settings
# from job_postings_crawler.repository.db_repository import QueueRepository


def verify_sf_api_key(
    sf_api_key: str = Query(
        title="Scraping Fish API Key",
        description="Scraping Fish API key. Only client's and test API keys work with this API.",
    ),
):
    return sf_api_key


async def create_postgres_pool() -> (
    AsyncGenerator[psycopg_pool.AsyncConnectionPool, None]
):
    async with psycopg_pool.AsyncConnectionPool(settings.POSTGRES_DSN) as pool:
        yield pool


async def create_mssql_pool() -> AsyncGenerator[aioodbc.Pool, None]:
    async with aioodbc.create_pool(dsn=settings.MSSQL_DSN) as pool:
        yield pool


async def repository(
    connection_pool: psycopg_pool.AsyncConnectionPool = Depends(create_postgres_pool),
    mssql_connection_pool: aioodbc.Pool = Depends(create_mssql_pool),
) -> AsyncGenerator[QueueRepository, None]:
    repo = QueueRepository(
        connection_pool=connection_pool,
        mssql_connection_pool=mssql_connection_pool,
        job_queue_max_retries=settings.URL_MAXIMUM_RETRIES,
    )

    yield repo
