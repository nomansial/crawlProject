import copy
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from config import settings
from logger import logger
from repository.models import CrawlResult, CreateJob, Job
from request_processor.request_handlers.handler import (
    UrlHandler,
)
from . import google_places_handler

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
                {"wait_for": "#text-input-what"},
                {
                    "input": {
                        "#text-input-what": job.metadata["search_term"],
                        "#text-input-where": job.metadata["location"],
                    }
                },
                {"click": "button[type='submit']"},
                {
                    "wait_for": {
                        "selector": "script#mosaic-data",
                        "state": "attached",
                    }
                },
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

    def process_content(self, response: httpx.Response, job: Job) -> List[CreateJob]:
        parsed_url = urlparse(response.headers.get("Resolved-Url"))
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        jobs = []
        soup = BeautifulSoup(response.text, "html.parser")

        jobs_details = soup.select(".jobTitle > a")
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
            "nav > ul > li > a[data-testid='pagination-page-next']"
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
        return self.process_content(response, job)

    def process_content(
        self, response: httpx.Response, job: Job
    ) -> Optional[List[CrawlResult]]:
        soup = BeautifulSoup(response.content, "html.parser")
        company_link = soup.select_one("div > span > a")
        if company_link is None:
            return None

        address_node = soup.select_one("#jobLocationText > div")
        address = None
        if address_node:
            address = address_node.text

        url = str(company_link["href"])
        name = company_link.text.strip()

        # Not specifying the value of data-testid attribute as it's the only h1 there
        position_node = soup.select_one("h1[data-testid] > span")
        position = None
        if position_node:
            position = position_node.text.strip()

        #adding job description 7/19/2024
        job_description_node = soup.find("div",id="jobDescriptionText")

        job_description = None
        if job_description_node:
            job_description = job_description_node.get_text(separator='\n')
        
        # Fixed Exception of compaddress 7/21/2024
        compaddress = None
        # Fixed Exception on placeId, latitude, longitude 7/23/2024
        placeid = None
        latitude = None
        longitude = None
        # update 7/20/2024 Getting Lat Long see further update in models (crawlResult)

        googleapikey = settings.GOOGLE_API_KEY
        location_data = google_places_handler.get_google_place_id(api_key=googleapikey,company_name=name)
        # Fixed Exception of location_data due to None 7/21/2024
        if location_data is not None:
            placeid = location_data[0]
            latitude = location_data[1]
            longitude = location_data[2]
            compaddress = location_data[3]

#add job description to results and directly to mssql
# added joib desceription 7/19/2024
        return [
            CrawlResult(
                input_id=job.input_id,
                crawl_id=job.crawl_id,
                url=url,
                company_name=name,
                address=compaddress,
                position=position,
                description= "",
                google_places_id=str(placeid),
                # fixed exception 7/23/2024
                lat = float(latitude) if latitude is not None else None,
                long = float(longitude) if longitude is not None else None,
                # added job_description 7/23/2024
                job_description= job_description
            )
        ]
