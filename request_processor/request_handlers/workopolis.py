import copy
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from logger import logger
from repository.models import CrawlResult, CreateJob, Job
from request_processor.request_handlers.handler import (
    UrlHandler,
)

# from job_postings_crawler.logger import logger
# from job_postings_crawler.repository.models import CrawlResult, CreateJob, Job
# from job_postings_crawler.request_processor.request_handlers.handler import (
#     UrlHandler,
# )


class SearchHandler(UrlHandler):
    async def handle_job(self, job: Job):
        if job.metadata.get("records_left", 0) <= 0:
            return None

        js_scenario = {
            "steps": [
                {"wait_for": "input[data-testid='findJobsKeywordInput']"},
                {
                    "input": {
                        "input[data-testid='findJobsKeywordInput']": job.metadata[
                            "search_term"
                        ],
                        "input[data-testid='findJobsLocationInput']": job.metadata[
                            "location"
                        ],
                    }
                },
                {"click": "button[data-testid='findJobsSearchSubmit']"},
                {"wait_for": "ul#job-list"},
            ]
        }

        response = await self.url_resolver.get(
            job.url,
            referer=job.metadata.get("referer"),
            js_scenario=js_scenario,
        )
        new_jobs = self.process_content(response, job)
        logger.debug(f"Adding {len(new_jobs)} new urls to queue")
        await self.repository.post_job(new_jobs)
        return None

    def process_content(
        self,
        response: httpx.Response,
        job: Job,
    ) -> List[CreateJob]:
        parsed_url = urlparse(response.headers.get("Resolved-Url"))
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        jobs = []
        soup = BeautifulSoup(response.text, "html.parser")

        jobs_details = soup.select("h2[data-testid='searchSerpJobTitle'] > a")
        records_left = job.metadata.get("records_left", 0)
        for job_details in jobs_details:
            if records_left <= 0:
                break
            new_metadata = copy.deepcopy(job.metadata)
            new_metadata["referer"] = response.headers.get("Resolved-Url")
            new_metadata["site_type"] = "details"

            job_details_url = urljoin(base_url, str(job_details["href"]))

            jobs.append(
                CreateJob(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=job_details_url,
                    metadata=new_metadata,
                )
            )
            records_left -= 1

        if records_left <= 0:
            return jobs
        next_page = soup.select_one(
            "nav > ul > li > a[data-testid='pageNumberBlockNext']"
        )
        new_metadata = copy.deepcopy(job.metadata)
        new_metadata["records_left"] = records_left
        if next_page:
            jobs.append(
                CreateJob(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=urljoin(base_url, str(next_page["href"])),
                    metadata=new_metadata,
                )
            )

        return jobs


class PageHandler(SearchHandler):
    async def handle_job(self, job: Job):
        if job.metadata.get("records_left", 0) <= 0:
            return None

        response = await self.url_resolver.get(
            job.url,
            referer=job.metadata.get("referer"),
        )
        new_jobs = self.process_content(response, job)
        logger.debug(f"Adding {len(new_jobs)} new urls to queue")
        await self.repository.post_job(new_jobs)
        return None


class DetailsHandler(UrlHandler):
    async def handle_job(self, job: Job) -> Optional[List[CrawlResult]]:
        response = await self.url_resolver.get(
            job.url, referer=job.metadata.get("referer")
        )
        return self.process_content(response, job.input_id, job.crawl_id)

    def process_content(
        self, response: httpx.Response, input_id: int, crawl_id: int
    ) -> Optional[List[CrawlResult]]:
        soup = BeautifulSoup(response.content, "html.parser")
        company_name = soup.select_one(
            "span[data-testid='viewJobCompanyName'] span[data-testid='detailText']"
        )
        if company_name is None:
            return None

        address_node = soup.select_one(
            "span[data-testid='viewJobCompanyLocation'] span[data-testid='detailText']"
        )
        address = None
        if address_node:
            address = address_node.text
        url = None
        name = company_name.text.strip()

        # Not specifying the value of data-testid attribute as it's the only h1 there
        position_node = soup.select_one("h1[data-testid]")
        position = None
        if position_node:
            position = position_node.text.strip()

        return [
            CrawlResult(
                input_id=input_id,
                crawl_id=crawl_id,
                url=url,
                company_name=name,
                address=address,
                position=position,
            )
        ]
