import asyncio
import base64
import datetime

from logger import logger
from repository.db_repository import QueueRepository
from repository.models import CreateJob, InputRow


async def crawl_scheduler(repository: QueueRepository, stop_flag: asyncio.Event):
    while not stop_flag.is_set():
        crawl = await repository.retrieve_pending_or_running_crawls()
        if crawl is None:
            await asyncio.sleep(5)
            continue

        logger.debug(f"Found crawl: {crawl.id}: {crawl.status}")
        if crawl.status == "loading":
            try:
                minimum_datetime = None
                if crawl.minimum_date is not None:
                    minimum_datetime = datetime.datetime.combine(
                        crawl.minimum_date, datetime.time.min
                    )
                input_rows = await repository.load_input_rows(
                    minimum_datetime, crawl.include_never_processed
                )
                create_jobs = [
                    map_input_row_to_job(row, crawl.id) for row in input_rows
                ]
                create_jobs = [job for job in create_jobs if job is not None]
                if len(create_jobs) == 0:
                    async with repository.transaction():
                        await repository.set_crawl_status(crawl.id, "done")
                        await repository.set_crawl_finished_at_to_now(crawl.id)
                    crawl.status = "done"
                    stop_flag.set()
                    continue
                async with repository.transaction():
                    await repository.post_job(create_jobs)
                    await repository.set_crawl_status(crawl.id, "processing")
                    crawl.status = "processing"
            except Exception as e:
                logger.exception(e)
                raise e

        while crawl.status == "processing" and not stop_flag.is_set():
            slept_for = 0
            while not stop_flag.is_set():
                await asyncio.sleep(10)
                slept_for += 10
                if slept_for >= 60:
                    break

            finished = await repository.is_crawl_finished()
            if finished:
                logger.info(f"Crawl {crawl.id} finished!")

                await repository.update_time_processed_datetimes(crawl.id)

                async with repository.transaction():
                    await repository.set_crawl_status(crawl.id, "done")
                    await repository.set_crawl_finished_at_to_now(crawl.id)
                    await repository.clear_queue()
                    crawl.status = "done"
                    stop_flag.set()

    if stop_flag.is_set() and crawl is not None:
        await repository.update_time_processed_datetimes(crawl.id)
        await repository.set_crawl_status(crawl.id, "done")
        await repository.set_crawl_finished_at_to_now(crawl.id)
        await repository.clear_queue()
        crawl.status = "done"

def encode_uule(location: str) -> str:
    encoded_location = location.encode()
    location_length = len(encoded_location)
    hashed = (
        base64.standard_b64encode(
            bytearray([8, 2, 16, 32, 34, location_length]) + encoded_location
        )
        .decode()
        .strip("=")
        .replace("+", "-")
        .replace("/", "_")
    )
    return f"w+{hashed}"


def map_input_row_to_job(row: InputRow, crawl_id: int) -> CreateJob | None:
    if row.website == "GoogleSearch" or row.website == "google":
        location = encode_uule(row.uule)

        url = f"https://www.google.com/search?gl={row.gl}&tbm=lcl&q={row.search_term}&uule={location}"
        return CreateJob(
            input_id=row.scraping_id,
            crawl_id=crawl_id,
            url=url,
            metadata={
                "records_left": row.record_limit,
                "search_term": row.search_term,
                "search_website": row.website,
                "location": location,
                "site_type": "search",
            },
        )

    if row.website == "Indeed":
        location = row.uule.replace(",", ", ")
        search_term = row.search_term
        if row.gl == "us":
            url = "https://www.indeed.com/?hl=en&co=us&countrySelector=1"
        else:
            url = f"https://{row.gl}.indeed.com"

        return CreateJob(
            input_id=row.scraping_id,
            crawl_id=crawl_id,
            url=url,
            metadata={
                "records_left": row.record_limit,
                "site_type": "search",
                "search_term": search_term,
                "search_website": row.website,
                "location": location,
            },
        )

    if row.website == "Workopolis":
        if row.gl != "ca":
            logger.warning(
                f"Workopolis only operates in Canada. Found row id {row.scraping_id} with gl = {row.gl}. This row will be skipped."
            )
            return None

        location = row.uule.replace(",", ", ")
        # Workopolis works only for CA anyway
        location = location.replace(", Canada", "")
        url = "https://www.workopolis.com/"
        return CreateJob(
            input_id=row.scraping_id,
            crawl_id=crawl_id,
            url=url,
            metadata={
                "records_left": row.record_limit,
                "site_type": "search",
                "search_term": row.search_term,
                "search_website": row.website,
                "location": location,
            },
        )

    raise ValueError(
        f"Unknown website found in input row with ScrapingID {row.scraping_id}: {row.website}"
    )
