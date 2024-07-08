#!/usr/bin/env python
import asyncio
import os
import signal
from typing import List

import aioodbc
import psycopg_pool

from config import settings
from crawler import crawl 
from logger import logger 
from url_resolver.scraping_fish import ScrapingFish 

stop_event = asyncio.Event()  # Define stop_event globally

def signal_handler(
    stop_flag: asyncio.Event,
    loop: asyncio.AbstractEventLoop,
    sig_count: List[int] = [0],
):
    if sig_count[0] == 0:
        sig_count[0] += 1
        logger.info(
            "Stopping. Letting current requests to finish (may take up to 7 minutes)... (To force quit, hit Ctrl-C again)"
        )
        stop_flag.set()
    else:
        logger.info("Forced shutdown (requests won't finish!)")
        loop.stop()
        os._exit(1)


async def wait_for_databases():
    while True:
        try:
            async with psycopg_pool.AsyncConnectionPool(
                settings.POSTGRES_DSN
            ) as connection_pool:
                async with connection_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()
            logger.info("Successfully connected to PostgreSQL database.")
            break
        except Exception as e:
            logger.info(
                "Couldn't connect to PostgreSQL database. It might not be ready yet. Retrying in 5 seconds... Reason:"
            )
            logger.exception(e)
            await asyncio.sleep(5)

    while True:
        try:
            async with aioodbc.create_pool(settings.MSSQL_DSN) as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()

            logger.info("Successfully connected to MS SQL database.")
            break
        except Exception as e:
            logger.info(
                "Couldn't connect to MS SQL database. It might not be ready yet. Retrying in 5 seconds... Reason:"
            )
            logger.exception(e)
            await asyncio.sleep(5)


async def main(stop_event: asyncio.Event):
    await wait_for_databases()

    scraping_fish_resolver = ScrapingFish(api_key=settings.SF_API_KEY)

    try:
        async with psycopg_pool.AsyncConnectionPool(
            settings.POSTGRES_DSN
        ) as connection_pool, aioodbc.create_pool(
            settings.MSSQL_DSN
        ) as mssql_connection_pool:
            await crawl(
                connection_pool=connection_pool,
                mssql_connection_pool=mssql_connection_pool,
                concurrency=settings.REQUESTS_CONCURRENCY,
                url_resolver=scraping_fish_resolver,
                url_maximum_retries=settings.URL_MAXIMUM_RETRIES,
                stop_event=stop_event,
            )
    except Exception as e:
        logger.error(f"Unexpected exception during crawl: {e}")
        logger.exception(e)


if __name__ == "__main__":
    # stop_event = asyncio.Event()
    loop = asyncio.new_event_loop()

    loop.add_signal_handler(signal.SIGTERM, signal_handler, stop_event, loop)
    loop.add_signal_handler(signal.SIGINT, signal_handler, stop_event, loop)

    try:
        loop.run_until_complete(main(stop_event))
    finally:
        loop.close()
