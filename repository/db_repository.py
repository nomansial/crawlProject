import datetime
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List

from dataclasses import dataclass
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
            
    # added function remove redundant data 7/21/2024 #updated 7/23/2024
    async def remove_duplicate_records(
        self
    ) :
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                        """
                       WITH MaxDateTimeCTE AS (
                        SELECT
		                    GooglePlacesID,
		                    CompanyName,
                            JobDescription,
                            MAX([DateTime]) AS MaxDateTime
                        FROM 
                            CRMWebScrapingResults
                        GROUP BY 
                            JobDescription,GooglePlacesID,CompanyName
                                    )

                        DELETE T
                        FROM CRMWebScrapingResults T
                        LEFT JOIN MaxDateTimeCTE CTE
                        ON T.GooglePlacesID = CTE.GooglePlacesID
                        AND T.CompanyName = CTE.CompanyName
                        AND T.JobDescription = CTE.JobDescription
                        AND T.[DateTime] = CTE.MaxDateTime
                        WHERE CTE.JobDescription IS NULL;
                        """
                    )

    #  added insert_data_into_CRMSuspectInput_Cache function 7/28/2024
    async def insert_data_into_CRMSuspectInput_Cache(
        self
    ) :
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                       INSERT INTO CRMSuspectInput_Cache (
                        Source,
                        ScrapingResultsID,
                        DateAdded,
                        CompanyName,
                        AddressFullTextOrig,
                        GooglePlacesID,
                        Phone,
                        Website,
                        lat,
                        long,
                        Position,
                        [Description],
                        [JobDescription],
                        DuplicateRow
                        )
                        SELECT 
                            src.SearchSite +' - '+ src.SearchTerm AS Source, 
                            src.ScrapingResultsID, 
                            GETDATE(), 
                            src.CompanyName, 
                            src.[Address], 
                            src.GooglePlacesID, 
                            src.Phone, 
                            src.[url], 
                            src.lat, 
                            src.long, 
                            src.Position, 
                            src.[Description], 
                            src.JobDescription, 
                            0
                        FROM 
                            CRMWebScrapingResults AS src
                        WHERE 
                            NOT EXISTS (
                                SELECT 1
                                FROM CRMSuspectInput_Cache AS dest
                                WHERE dest.CompanyName = src.CompanyName
                            );

                        -- Delete records from CRMWebScrapingResults if company name exists in CRMSuspectInput_Cache
                        DELETE src
                        FROM CRMWebScrapingResults AS src
                        WHERE EXISTS (
                            SELECT 1
                            FROM CRMSuspectInput_Cache AS dest
                            WHERE dest.CompanyName = src.CompanyName
                        );

                    """
                    )
                # rows = await cursor.fetchall()
                
     
 



    # Get Data from crmscraping results to show on transfer page 7/30/2024 update row count 7/31/2024
    async def get_results(
        self,
    ) -> List[CrawlResult]:
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                   

                    SELECT 
                    ScrapingResultsID,
                    ScrapingID,
                    url,
                    CompanyName AS company_name,
                    Address AS address,
                    Phone AS phone_number,
                    GooglePlacesID AS google_places_id,
                    Position AS position,
                    SearchSite,
					SearchTerm,
                    Description AS description,
                    lat,
                    long,
                    JobDescription AS job_description,
                    COUNT(*) OVER() AS record_count,
				   (SELECT 
						COUNT(*) AS total_duplicate_records
					FROM (
						SELECT 
							WebScraping.CompanyName
						FROM 
							"CRMWebScrapingResults" AS WebScraping
						INNER JOIN 
							"CRMSuspectInput_Cache" AS SuspectInput
						ON 
							WebScraping.CompanyName = SuspectInput.CompanyName
						GROUP BY 
							WebScraping.CompanyName
					) AS Duplicates) AS DuplicateRecord
					FROM 
                    "CRMWebScrapingResults";

                    """,
                   
                )
                crawlresults = await cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                results = [
                CrawlResult(
                    input_id= row[column_names.index('ScrapingResultsID')],
                    crawl_id= row[column_names.index('ScrapingID')],
                    url=row[column_names.index('url')],
                    company_name=row[column_names.index('company_name')],
                    address=row[column_names.index('address')],
                    phone_number=row[column_names.index('phone_number')],
                    google_places_id=row[column_names.index('google_places_id')],
                    position=row[column_names.index('position')],
                    description=row[column_names.index('description')],
                    lat=row[column_names.index('lat')],
                    long=row[column_names.index('long')],
                    job_description=row[column_names.index('job_description')],
                    record_count = row[column_names.index('record_count')],
                    duplicate_record_count= row[column_names.index('DuplicateRecord')],
                    search_term=row[column_names.index('SearchTerm')],
                    search_site=row[column_names.index('SearchSite')]


                    ) for row in crawlresults
                ]
            
            return results

    # added get_results_CRMSuspectInput_Cache 7/31/2024
    async def get_results_CRMSuspectInput_Cache(
        self,
    ) -> List[CrawlResult]:
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    WITH DuplicateCounts AS (
                            SELECT 
                                CompanyName,
                                COUNT(*) AS total_duplicate_records
                            FROM 
                                "CRMSuspectInput_Cache"
                            GROUP BY 
                                CompanyName
                            HAVING 
                                COUNT(*) > 1
                        ),
                        TotalDuplicateRecords AS (
                            SELECT 
                                SUM(total_duplicate_records) AS total_duplicate_records
                            FROM 
                                DuplicateCounts
                        )
                    SELECT 
                        c.ScrapingResultsID as InputID ,
                        c.Source AS Source,
                        c.InputID AS ScrapingId,
                        c.WebSite as url,
                        c.CompanyName AS company_name,
                        c.AddressFullTextOrig AS address,
                        c.Phone AS phone_number,
                        c.GooglePlacesID AS google_places_id,
                        c.Position AS position,
                        c.Description AS description,
                        c.lat,
                        c.long,
                        c.JobDescription AS job_description,
                        COUNT(*) OVER() AS record_count,
                        (SELECT total_duplicate_records FROM TotalDuplicateRecords) AS DuplicateRecord
                    FROM 
                        "CRMSuspectInput_Cache" c;

                    """,
                   
                )
                crawlresults = await cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                results = [
                CrawlResult(
                    input_id= row[column_names.index('InputID')],
                    crawl_id= row[column_names.index('ScrapingId')],
                    url=row[column_names.index('url')],
                    company_name=row[column_names.index('company_name')],
                    address=row[column_names.index('address')],
                    phone_number=row[column_names.index('phone_number')],
                    google_places_id=row[column_names.index('google_places_id')],
                    position=row[column_names.index('position')],
                    description=row[column_names.index('description')],
                    lat=row[column_names.index('lat')],
                    long=row[column_names.index('long')],
                    job_description=row[column_names.index('job_description')],
                    record_count = row[column_names.index('record_count')],
                    duplicate_record_count= row[column_names.index('DuplicateRecord')],
                    search_term= row[column_names.index('Source')]
                    ) for row in crawlresults
                ]
            
            return results
      
     #added duplication removal for crmsuspect input cache 7/31/2024

    async def remove_duplicate_crmsuspectcache(
        self
    ) :
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
              await cursor.execute("""
                                WITH LatestRecordsCTE AS (
                    SELECT
                        [InputID],
                        [CompanyName],
                        ROW_NUMBER() OVER (
                            PARTITION BY [CompanyName]
                            ORDER BY [DateAdded] DESC
                        ) AS rn
                    FROM 
                        CRMSuspectInput_Cache
                )

                -- Step 2: Delete records that are not the most recent
                DELETE T
                FROM CRMSuspectInput_Cache T
                WHERE T.[InputID] IN (
                    SELECT [InputID]
                    FROM LatestRecordsCTE
                    WHERE rn > 1
                );

                -- Step 3: Return the number of removed rows
                SELECT 
                    COUNT(*) AS removed_row_count
                FROM LatestRecordsCTE
                WHERE rn > 1;
                """)

            return True
                    
    # added funnction to clean job description 7/23/2024
    async def clean_job_description(
        self
    ) :
        async with self.mssql_connection_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                        """
                        UPDATE [CRMWebScrapingResults]
                        SET JobDescription= 
                            CASE 
                                WHEN LEFT(JobDescription, 1) = CHAR(13) THEN LTRIM(SUBSTRING(JobDescription, PATINDEX('%[^' + CHAR(13) + ' ' + CHAR(10) + ']%', JobDescription), LEN(JobDescription)))
                                WHEN LEFT(JobDescription, 1) = CHAR(10) THEN LTRIM(SUBSTRING(JobDescription, PATINDEX('%[^' + CHAR(10) + ' ' + CHAR(13) + ']%', JobDescription), LEN(JobDescription)))
                                ELSE LTRIM(JobDescription)
                            END;

                        """
                    )
                     

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

                # #job description 7/19/2024
                description = result.description if hasattr(result,'description') else None

                # added job_description 7/23/2024
                job_description = result.job_description if hasattr(result,'job_description') else None
                

                #update added lat long 7/20/2024
                lat = result.lat if hasattr(result,'lat') else None
                long = result.long if hasattr(result,'long') else None
                # updated: > added job description after position
                await cursor.execute(
                    """
                    INSERT INTO CRMWebScrapingResults (ScrapingID, DateTime, CompanyName, url, Address, Phone,lat,long, GooglePlacesID, Position,Description, SearchSite, SearchTerm,RecordLimit, JobDescription)
                    VALUES (?, GETDATE(), ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)
                """,
                    (
                        input_id,
                        company_name,
                        url,
                        address,
                        phone_number,
                        lat,
                        long,
                        google_places_id,
                        position,
                        description,
                        metadata.get('search_website'),
                        metadata.get('search_term'),
                        metadata.get('records_left'),
                        # added job_description 7/23/2024
                        job_description
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
