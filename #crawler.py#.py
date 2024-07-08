import asyncio

import aioodbc
import psycopg_pool
import uvicorn

from job_postings_crawler.config import settings
from job_postings_crawler.crawl_scheduler import crawl_scheduler
from job_postings_crawler.logger import logger
from job_postings_crawler.repository.db_repository import QueueRepository
from job_postings_crawler.request_processor.request_processor import process_request
from job_postings_crawler.url_resolver.base import UrlResolver
from job_postings_crawler.web_ui.app import app


async def crawl(
    connection_pool: psycopg_pool.AsyncConnectionPool,
    mssql_connection_pool: aioodbc.Pool,
    concurrency: int,
    url_resolver: UrlResolver,
    stop_event: asyncio.Event,
    url_maximum_retries: int,
):
    await _init_db(connection_pool)
    logger.debug("DB ready")
    crawler_repo = QueueRepository(
        connection_pool=connection_pool,
        mssql_connection_pool=mssql_connection_pool,
        job_queue_max_retries=url_maximum_retries,
    )
    logger.debug("Starting tasks")
    async with asyncio.TaskGroup() as tg:
        results_queue = asyncio.Queue()
        for i in range(concurrency):
            tg.create_task(
                process_request(
                    repository=crawler_repo,
                    url_resolver=url_resolver,
                    url_maximum_retries=url_maximum_retries,
                    stop_flag=stop_event,
                    results_queue=results_queue,
                ),
                name=f"crawler-task-{i}",
            )

        tg.create_task(
            crawl_scheduler(repository=crawler_repo, stop_flag=stop_event),
            name="crawl-scheduler",
        )

        config = uvicorn.Config(app=app, host="0.0.0.0", port=settings.PORT, loop="asyncio")
        server = uvicorn.Server(config)
        tg.create_task(server.serve(), name="http-server")


async def _init_db(connection_pool: psycopg_pool.AsyncConnectionPool):
    async with connection_pool.connection() as conn:
        async with conn.cursor() as cur:
            with open("sql/create_tables.sql", "r") as f:
                await cur.execute(f.read())
        await conn.commit()
