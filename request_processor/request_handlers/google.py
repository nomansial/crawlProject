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
    async def handle_job(self, job: Job) -> Optional[List[CrawlResult]]:
        response = await self.url_resolver.get(
            job.url,
            referer=job.metadata.get("referer"),
            js_scenario={
                "steps": [
                    {"wait": 2000},
                    {"click_if_exists": "button[aria-label='Reject all']"},
                    {"wait_for": "div#rl_ist0"},
                ]
            },
        )
        jobs = self.process_content(response, job)

        await self.repository.post_job(jobs)

        return jobs

    def process_content(
        self,
        response: httpx.Response,
        job: Job,
    ) -> List[CreateJob]:
        jobs = []
        soup = BeautifulSoup(response.text, "html.parser")
        business_details_links = soup.select(
            "div:not([data-is-ad]) > div > div > div > a.rllt__link"
        )
        records_left = job.metadata.get("records_left", 0)
        for business_details in business_details_links:
            if records_left <= 0:
                break

            data_cid = business_details.get("data-cid")

            new_metadata = copy.deepcopy(job.metadata)
            new_metadata["site_type"] = "details"
            new_metadata["data_cid"] = data_cid
            jobs.append(
                CreateJob(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=job.url,
                    metadata=new_metadata,
                )
            )
            records_left -= 1

        if records_left <= 0:
            return jobs

        next_page = soup.select_one("a#pnnext")
        if next_page:
            new_metadata = copy.deepcopy(job.metadata)
            new_metadata["records_left"] = records_left

            parsed_url = urlparse(response.headers.get("Resolved-Url"))
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            jobs.append(
                CreateJob(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=urljoin(base_url, str(next_page["href"])),
                    metadata=new_metadata,
                )
            )

        return jobs


class DetailsHandler(UrlHandler):
    async def handle_job(self, job: Job):
        data_cid = job.metadata.get("data_cid")
        response = await self.url_resolver.get(
            job.url,
            referer=job.metadata.get("referer"),
            js_scenario={
                "steps": [
                    {"wait": 2000},
                    {"click_if_exists": "button[aria-label='Reject all']"},
                    {"wait_for": f"a[data-cid='{data_cid}']"},
                    {"click": f"a[data-cid='{data_cid}']"},
                    {"wait_for": "div.xpdopen"},
                ]
            },
        )
        return self.process_content(response, job)

    def process_content(
        self,
        response: httpx.Response,
        job: Job,
    ) -> List[CrawlResult]:
        soup = BeautifulSoup(response.text, "html.parser")
        google_places_id = job.metadata.get("data_cid")

        company_name_node = soup.select_one("h2[data-attrid='title']")
        company_name = None
        if company_name_node is not None:
            company_name = company_name_node.text.strip()

        company_address_node = soup.select_one(
            "div[data-attrid='kc:/location/location:address'] span.LrzXr"
        )
        company_address = None
        if company_address_node is not None:
            company_address = company_address_node.text.strip()

        phone_number_node = soup.select_one("span[aria-label*='Call Phone Number']")
        phone_number = None
        if phone_number_node is not None:
            phone_number = phone_number_node.text.strip()

        url = None
        url_node = soup.select_one("div.bkaPDb:nth-child(1) > a:nth-child(1)")
        if url_node is not None:
            url = url_node.get("href")
            if url is not None and isinstance(url, list):
                url = url[0]

        res = CrawlResult(
            input_id=job.input_id,
            crawl_id=job.crawl_id,
            url=url,
            google_places_id=google_places_id,
            company_name=company_name,
            address=company_address,
            phone_number=phone_number,
        )

        if (
            res.address is None
            and res.company_name is None
            and res.phone_number is None
            and res.url is None
        ):
            raise ValueError(f"Invalid google result, everything is None: {res}")

        logger.info(f"GOOGLE RESULT: {res}")
        return [res]
