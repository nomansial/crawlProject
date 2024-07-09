import datetime
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List

import aioodbc
import psycopg
import psycopg_pool
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

from logger import logger
from repository.models import (
    Crawl,
    CrawlResult,
    CreateCrawl,
    CreateJob,
    InputRow,
    Job,
)

# from job_postings_crawler.logger import logger
# from job_postings_crawler.repository.models import (
#     Crawl,
#     CrawlResult,
#     CreateCrawl,
#     CreateJob,
#     InputRow,
#     Job,
# )


class QueueRepository:
    connection_pool: psycopg_pool.AsyncConnectionPool
    mssql_connection_pool: aioodbc.Pool

    def __init__(
        self,
        connection_pool: psycopg_pool.AsyncConnectionPool,
        mssql_connection_pool: aioodbc.Pool,
        job_queue_max_retries: int,
    ) -> None:
        super().__init__()
        self.connection_pool = connection_pool
        self.mssql_connection_pool = mssql_connection_pool
        self.maximum_retries = job_queue_max_retries

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[psycopg.AsyncConnection, None]:
        async with self.connection_pool.connection() as conn:
            async with conn.transaction():
                yield conn

    async def retrieve_job(
        self, conn: psycopg.AsyncConnection | None = None
    ) -> Job | None:
        async def _retrieve_job(conn: psycopg.AsyncConnection) -> Job | None:
            job = None
            async with conn.cursor(row_factory=class_row(Job)) as cur:
                await cur.execute(
                    """
                            SELECT url, input_id, crawl_id, metadata, status, attempt, id FROM url_queue
                            WHERE status = 'pending'
                            OR (status = 'error' AND attempt < %s)
                            OR (status = 'processing' AND updated_at < current_timestamp - INTERVAL '15 minutes')
                            ORDER BY RANDOM()
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                            """,
                    [self.maximum_retries],
                )
                job = await cur.fetchone()
                if job:
                    await cur.execute(
                        """
                                UPDATE url_queue
                                SET status = 'processing'
                                WHERE id = %s;
                                """,
                        [job.id],
                    )
            return job

        if conn is None:
            async with self.transaction() as conn:
                return await _retrieve_job(conn)
        return await _retrieve_job(conn)

    async def post_job(
        self, jobs: List[CreateJob], conn: psycopg.AsyncConnection | None = None
    ) -> None:
        if not jobs:
            return

        async def _post_job(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO url_queue (url, metadata, status, input_id, crawl_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        [
                            job.url,
                            Jsonb(job.metadata),
                            job.status or "pending",
                            job.input_id,
                            job.crawl_id,
                        ]
                        for job in jobs
                    ],
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _post_job(conn)
        return await _post_job(conn)

    async def post_job_to_dead_letter(
        self, job: CreateJob, conn: psycopg.AsyncConnection | None = None
    ):
        logger.debug(f"Posting job to dead letter queue: {job.url}")

        async def _post_job_to_dead_letter(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
                        INSERT INTO url_dead_letter_queue(url, metadata)
                        VALUES (%s, %s)
                    """,
                        [
                            job.url,
                            Jsonb(job.metadata),
                        ],
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to post job {str(job)} to dead letter queue:", e
                    )

        if conn is None:
            async with self.transaction() as conn:
                return await _post_job_to_dead_letter(conn)
        return await _post_job_to_dead_letter(conn)

    async def set_success(
        self, job_id: int, conn: psycopg.AsyncConnection | None = None
    ):
        async def _set_success(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE url_queue SET status=%s WHERE id=%s
                    """,
                    ["done", job_id],
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _set_success(conn)
        return await _set_success(conn)

    async def set_error(
        self,
        job_id: int,
        attempt: int,
        error: Any,
        conn: psycopg.AsyncConnection | None = None,
    ):
        async def _set_error(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE url_queue SET status=%s, attempt=%s, last_error=%s WHERE id=%s 
                    """,
                    ["error", attempt, f"{type(error)}: {error}", job_id],
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _set_error(conn)

        return await _set_error(conn)

    async def get_crawls(
        self,
        conn: psycopg.AsyncConnection | None = None,
        limit: int = 50,
    ) -> List[Crawl]:
        async def _get_crawls(conn: psycopg.AsyncConnection):
            async with conn.cursor(row_factory=class_row(Crawl)) as cur:
                await cur.execute(
                    """
                    SELECT id, created_at, finished_at, include_never_processed, minimum_date, status FROM crawls ORDER BY created_at DESC LIMIT %s;
                    """,
                    [limit],
                )
                crawls = await cur.fetchall()
                return crawls

        if conn is None:
            async with self.transaction() as conn:
                return await _get_crawls(conn)
        return await _get_crawls(conn)

    async def get_crawl(
        self, crawl_id: int, conn: psycopg.AsyncConnection | None = None
    ) -> Crawl | None:
        async def _get_crawl(conn: psycopg.AsyncConnection):
            async with conn.cursor(row_factory=class_row(Crawl)) as cur:
                await cur.execute(
                    """
                SELECT id, status, minimum_date, created_at, updated_at FROM crawls
                WHERE id = %s;
                """,
                    [crawl_id],
                )
                crawl = await cur.fetchone()
                return crawl

        if conn is None:
            async with self.transaction() as conn:
                return await _get_crawl(conn)

        return await _get_crawl(conn)

    # async def retrieve_pending_or_running_crawls(
    #     self, conn: psycopg.AsyncConnection | None = None
    # ) -> Crawl | None:
    #     async def _retrieve_pending_or_running_crawls(conn: psycopg.AsyncConnection):
    #         async with conn.cursor(row_factory=class_row(Crawl)) as cur:
    #             await cur.execute(
    #                 """
    #                 SELECT id, status, minimum_date, include_never_processed, created_at, updated_at FROM crawls
    #                 WHERE status in ('pending', 'processing', 'loading')
    #                 LIMIT 1
    #                 FOR UPDATE SKIP LOCKED;
    #                 """
    #             )
    #             crawl = await cur.fetchone()
    #             if crawl and crawl.status == "pending":
    #                 await cur.execute(
    #                     """
    #                     UPDATE crawls
    #                     SET status = 'loading'
    #                     WHERE id = %s;
    #                     """,
    #                     [crawl.id],
    #                 )
    #                 crawl.status = "loading"
    #             return crawl
                

    #     if conn is None:
    #         async with self.transaction() as conn:
    #             return await _retrieve_pending_or_running_crawls(conn)
    #     return await _retrieve_pending_or_running_crawls(conn)

    
    async def retrieve_pending_or_running_crawls(
        self, conn: psycopg.AsyncConnection | None = None
    ) -> Crawl | None:
        async def _retrieve_pending_or_running_crawls(conn: psycopg.AsyncConnection):
            async with conn.cursor(row_factory=class_row(Crawl)) as cur:
                await cur.execute(
                    """
                    SELECT id, status, minimum_date, include_never_processed, created_at, updated_at FROM crawls
                    WHERE status in ('pending', 'processing', 'loading')
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED;
                    """
                )
                crawl = await cur.fetchone()
                if crawl and crawl.status == "pending" :
                    await cur.execute(
                        """
                        UPDATE crawls
                        SET status = 'loading'
                        WHERE id = %s;
                        """,
                        [crawl.id],
                    )
                    crawl.status = "loading"
                elif crawl is None:
                    scheduleCrawler = await self.schedule_crawl(crawl= CreateCrawl(include_never_processed=True ))
                    if scheduleCrawler:
                        await self.retrieve_pending_or_running_crawls()
                return crawl
                

        if conn is None:
            async with self.transaction() as conn:
                return await _retrieve_pending_or_running_crawls(conn)
        return await _retrieve_pending_or_running_crawls(conn)

    async def clear_queue(self, conn: psycopg.AsyncConnection | None = None):
        async def _clear_queue(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM url_queue;
                    """
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _clear_queue(conn)

        return await _clear_queue(conn)

    async def set_crawl_status(
        self, crawl_id: int, status: str, conn: psycopg.AsyncConnection | None = None
    ):
        async def _set_crawl_status(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE crawls
                    SET status = %s
                    WHERE id = %s;
                    """,
                    [status, crawl_id],
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _set_crawl_status(conn)

        return await _set_crawl_status(conn)

    async def set_crawl_finished_at_to_now(
        self, crawl_id: int, conn: psycopg.AsyncConnection | None = None
    ):
        async def _set_crawl_finished_at_to_now(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE crawls
                    SET finished_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    [crawl_id],
                )

        if conn is None:
            async with self.transaction() as conn:
                return await _set_crawl_finished_at_to_now(conn)

        return await _set_crawl_finished_at_to_now(conn)

    async def is_crawl_finished(self, conn: psycopg.AsyncConnection | None = None):
        async def _is_crawl_finished(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT COUNT(*) FROM url_queue
                    WHERE status IN ('pending', 'processing');
                    """
                )
                unfinished_count = await cur.fetchone()
                return unfinished_count is not None and unfinished_count[0] == 0

        if conn is None:
            async with self.transaction() as conn:
                return await _is_crawl_finished(conn)
        return await _is_crawl_finished(conn)

    async def load_input_rows(
        self,
        minimum_date: datetime.datetime | None = None,
        include_never_processed: bool = False,
    ) -> List[InputRow]:
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT ScrapingID, Website, SearchTerm, RecordLimit, gl, uule
                    FROM CRMWebScraping
                """
                if minimum_date and include_never_processed:
                    query += " WHERE TimeProcessed IS NULL OR TimeProcessed >= ?"
                    await cursor.execute(query, (minimum_date,))
                elif minimum_date:
                    query += " WHERE TimeProcessed >= ?"
                    await cursor.execute(query, (minimum_date,))
                elif include_never_processed:
                    query += " WHERE TimeProcessed IS NULL"
                    await cursor.execute(query)
                else:
                    # This case should be handled at the endpoint level, but added for safety
                    raise ValueError(
                        "Either minimum_date must be set or include_never_processed must be true."
                    )

                rows = await cursor.fetchall()
                return [InputRow(*row) for row in rows]
            
    # async def count_input_rows(
    # self,
    # minimum_date: datetime.datetime | None = None,
    # include_never_processed: bool = False,
    # ) -> int:
    #     async with self.mssql_connection_pool.acquire() as conn:
    #         async with conn.cursor() as cursor:
    #             query = """
    #                 SELECT COUNT(*) 
    #                 FROM [ActiveM_DemoERP].[dbo].[CRMWebScraping]
    #             """
    #             if minimum_date and include_never_processed:
    #                 query += " WHERE TimeProcessed IS NULL OR TimeProcessed >= ?"
    #                 await cursor.execute(query, (minimum_date,))
    #             elif minimum_date:
    #                 query += " WHERE TimeProcessed >= ?"
    #                 await cursor.execute(query, (minimum_date,))
    #             elif include_never_processed:
    #                 query += " WHERE TimeProcessed IS NULL"
    #                 await cursor.execute(query)
    #             else:
    #                 # This case should be handled at the endpoint level, but added for safety
    #                 raise ValueError(
    #                     "Either minimum_date must be set or include_never_processed must be true."
    #                 )

    #             row = await cursor.fetchone()
    #             return row[0] if row else 0
            

    async def count_input_rows(self) -> int:
        async with self.mssql_connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """
                        SELECT COUNT(*) 
                        FROM CRMWebScraping 
                        WHERE TimeProcessed IS NULL
                        """
                    )
                    row = await cursor.fetchone()
                    # Since cursor.fetchone() returns a tuple, we need to get the first element
                    count = row[0] if row else 0
                    return count
                
    async def schedule_crawl(
        self, crawl: CreateCrawl, conn: psycopg.AsyncConnection | None = None
    ):
        async def _schedule_crawl(conn: psycopg.AsyncConnection):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                SELECT COUNT(*) FROM crawls
                WHERE status in ('processing', 'loading');
                """
                )
                count = await cur.fetchone()
                if count is not None and count[0] > 0:
                    return False

                await cur.execute(
                    """
                INSERT INTO crawls(minimum_date, include_never_processed)
                VALUES (%s, %s);
                """,
                    [crawl.minimum_date, crawl.include_never_processed],
                )
                return True

        if conn is None:
            async with self.transaction() as conn:
                return await _schedule_crawl(conn)

        return await _schedule_crawl(conn)

    async def write_result(self, result: CrawlResult, metadata):
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:


                # Check if result object has other properties
                company_name = result.company_name if hasattr(result, 'company_name') else None
                input_id = result.input_id if hasattr(result, 'input_id') else None
                url = result.url if hasattr(result, 'url') else None
                address = result.address if hasattr(result, 'address') else None
                phone_number = result.phone_number if hasattr(result, 'phone_number') else None
                google_places_id = result.google_places_id if hasattr(result, 'google_places_id') else None
                position = result.position if hasattr(result, 'position') else None

                await cursor.execute(
                    """
                    INSERT INTO CRMWebScrapingResults (ScrapingID, DateTime, CompanyName, url, Address, Phone, GooglePlacesID, Position, SearchSite, SearchTerm,RecordLimit)
                    VALUES (?, GETDATE(), ?, ?, ?, ?, ?, ?,?,?,?)
                """,
                    (
                        input_id,
                        company_name,
                        url,
                        address,
                        phone_number,
                        google_places_id,
                        position,
                        metadata.get('search_website'),
                        metadata.get('search_term'),
                        metadata.get('records_left'),
                    ),
                )
                await conn.commit()

    async def update_time_processed_datetimes(self, crawl_id: int):
        async with self.transaction() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                SELECT DISTINCT input_id FROM url_queue
                WHERE crawl_id = %s AND status = 'done';
                """,
                    [crawl_id],
                )
                input_ids = await cur.fetchall()
                input_ids = [row[0] for row in input_ids]

        async with self.mssql_connection_pool.acquire() as conn:
            input_ids_str = ",".join(map(str, input_ids))
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"""
                    UPDATE CRMWebScraping
                    SET TimeProcessed = SYSUTCDATETIME()
                    WHERE ScrapingID IN ({input_ids_str}) AND TimeProcessed IS NULL;
                """
                )
                await conn.commit()

    async def update_time_processed(self, input_id: int):
        
        async with self.mssql_connection_pool.acquire() as conn:
            # input_ids_str = ",".join(map(str, input_ids))
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"""
                    UPDATE CRMWebScraping
                    SET TimeProcessed = SYSUTCDATETIME()
                    WHERE ScrapingID = ({input_id});
                """
                )
                await conn.commit()
