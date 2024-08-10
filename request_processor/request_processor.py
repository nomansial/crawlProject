import asyncio
from datetime import datetime
import time


from logger import logger
from repository.db_repository import QueueRepository
from request_processor.request_dispatcher import dispatch_request
from url_resolver.base import UrlResolver

# from job_postings_crawler.logger import logger
# from job_postings_crawler.repository.db_repository import QueueRepository
# from job_postings_crawler.request_processor.request_dispatcher import dispatch_request
# from job_postings_crawler.url_resolver.base import UrlResolver

# Define global elapsed_time variable
global elapsed_time
elapsed_time = None

async def process_request(
    repository: QueueRepository,
    url_resolver: UrlResolver,
    url_maximum_retries: int,
    stop_flag: asyncio.Event,
):
    global elapsed_time
    while not stop_flag.is_set():
        job = await repository.retrieve_job()

        if job is None:
            await asyncio.sleep(2)
            continue
        logger.info(
            f"Crawling: {job.url} (type: {job.metadata.get('site_type')}, search_term: {job.metadata.get('search_term')}, location: {job.metadata.get('location')})"
        )
        try:
            current_time = time.time()
            results = await dispatch_request(job, repository, url_resolver)
            current_time_after_results = time.time()
            elapsed_time =  current_time_after_results - current_time
            if results:
                for result in results:
                    await repository.write_result(result, job.metadata)
                    await repository.write_result(result, job.metadata)
                await repository.update_time_processed(input_id=job.input_id)
            await repository.set_success(job.id)
            
        except Exception as err:
            logger.error(
                f"Error processing url: {job.url}: {type(err)}: {err}. Setting job {job.id} status to error."
            )
            logger.exception(err)

            if job.attempt == url_maximum_retries:
                await repository.post_job_to_dead_letter(job)
            else:
                await repository.set_error(job.id, job.attempt + 1, err)

    logger.info("Task finished")

def getElapsedTime():
    return elapsed_time

