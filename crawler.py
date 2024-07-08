import asyncio
import random

import aioodbc
import psycopg_pool
import uvicorn

from config import settings
from crawl_scheduler import crawl_scheduler
from logger import logger
from repository.db_repository import QueueRepository
from request_processor.request_processor import process_request
from url_resolver.base import UrlResolver
from web_ui.app import app


async def crawl(
    connection_pool: psycopg_pool.AsyncConnectionPool,
    mssql_connection_pool: aioodbc.Pool,
    concurrency: int,
    url_resolver: UrlResolver,
    stop_event: asyncio.Event,
    url_maximum_retries: int,
):
    # Revert this back on production
    # await _init_db(connection_pool, mssql_connection_pool)
    logger.debug("DB ready")
    crawler_repo = QueueRepository(
        connection_pool=connection_pool,
        mssql_connection_pool=mssql_connection_pool,
        job_queue_max_retries=url_maximum_retries,
    )
    logger.debug("Starting tasks")
    async with asyncio.TaskGroup() as tg:
        for i in range(concurrency):
            tg.create_task(
                process_request(
                    repository=crawler_repo,
                    url_resolver=url_resolver,
                    url_maximum_retries=url_maximum_retries,
                    stop_flag=stop_event,
                ),
                name=f"crawler-task-{i}",
            )

        tg.create_task(
            crawl_scheduler(repository=crawler_repo, stop_flag=stop_event),
            name="crawl-scheduler",
        )

        config = uvicorn.Config(
            # Revert back to this on production
            # app=app, host="0.0.0.0", port=settings.PORT, loop="asyncio"
            app=app, host="0.0.0.0", port=random.randint(1024, 65535), loop="asyncio"
        )
        server = uvicorn.Server(config)
        tg.create_task(server.serve(), name="http-server")


async def _init_db(
    connection_pool: psycopg_pool.AsyncConnectionPool,
    mssql_connection_pool: aioodbc.Pool,
):
    async with connection_pool.connection() as conn:
        async with conn.cursor() as cur:
            with open("sql/create_tables.sql", "r") as f:
                await cur.execute(f.read())
        await conn.commit()

    async with mssql_connection_pool.acquire() as conn:
        async with conn.cursor() as cur:
            with open("sql/mssql/create_tables.sql", "r") as f:
                await cur.execute(f.read())
                await conn.commit()
