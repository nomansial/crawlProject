from typing import List, Optional

from repository.db_repository import QueueRepository
from repository.models import CrawlResult, Job
from request_processor.request_handlers import (
    glassdoor,
    google,
    indeed,
    workopolis,
)
from url_resolver.base import UrlResolver

# from job_postings_crawler.repository.db_repository import QueueRepository
# from job_postings_crawler.repository.models import CrawlResult, Job
# from job_postings_crawler.request_processor.request_handlers import (
#     google,
#     indeed,
#     workopolis,
# )
# from job_postings_crawler.url_resolver.base import UrlResolver


async def dispatch_request(
    job: Job,
    repository: QueueRepository,
    url_resolver: UrlResolver,
) -> Optional[List[CrawlResult]]:
    if "google.com" in job.url:
        # NOTE: In practice, both handlers access the same website. They've got split into two
        # separate handlers to clearly separate initial search from details extraction.
        # SearchHandler does NOT emit first search result as crawling result.
        match job.metadata.get("site_type"):
            case "search":
                return await google.SearchHandler(repository, url_resolver).handle_job(
                    job
                )
            case "details":
                return await google.DetailsHandler(repository, url_resolver).handle_job(
                    job
                )
    if "indeed.com" in job.url:
        match job.metadata.get("site_type"):
            case "search":
                return await indeed.SearchHandler(repository, url_resolver).handle_job(
                    job
                )
            case "list":
                return await indeed.PageHandler(repository, url_resolver).handle_job(
                    job
                )
            case "details":
                return await indeed.DetailsHandler(repository, url_resolver).handle_job(
                    job
                )
            
    if "glassdoor.com" in job.url:
        match job.metadata.get("site_type"):
            case "search":
                return await glassdoor.SearchHandler(repository, url_resolver).handle_job(
                    job
                )
            case "list":
                return await glassdoor.PageHandler(repository, url_resolver).handle_job(
                    job
                )
            case "details":
                return None
                # return await glassdoor.DetailsHandler(repository, url_resolver).handle_job(
                #     job
                # )

    if "workopolis.com" in job.url:
        match job.metadata.get("site_type"):
            case "search":
                return await workopolis.SearchHandler(
                    repository, url_resolver
                ).handle_job(job)
            case "list":
                return await workopolis.PageHandler(
                    repository, url_resolver
                ).handle_job(job)
            case "details":
                return await workopolis.DetailsHandler(
                    repository, url_resolver
                ).handle_job(job)
    raise ValueError(
        f"Don't know how to handle url {job.url}, metadata: {job.metadata}"
    )  # TODO: Error type could be better
