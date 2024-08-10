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

import requests
from bs4 import BeautifulSoup

# Replace 'YOUR_API_KEY' with your actual ScrapingFish API key
API_KEY = 'cyUbfaD2vOUgyrNLe2QGTgudk8W12Hzy8fTIuRJGMOeItyYvt0MBid6QaEgjkExOtL5umQAE0Vgn18AtzK'
SEARCH_URL = 'https://scraping.narf.ai/api/v1/'
def scrape_glassdoor(query, location_name = None):
    # Step 1: Define the URLs
    if location_name is not None:
        glassdoor_url = (
            f"https://www.glassdoor.com/Job/jobs.htm?"
            f"sc.keyword={query.replace(' ', '%20')}&"
            f"locT=C&locKeyword={location_name.replace(' ', '%20')}"
        )
    else:
        glassdoor_url = (
            f"{query}"
        )
        
    api_url = f"{SEARCH_URL}?api_key={API_KEY}&url={glassdoor_url}"

   
    # Step 4: Parse the search results with BeautifulSoup
    try:
        if location_name is not None:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an exception for HTTP errors
        else:
            response = requests.get(glassdoor_url)
            response.raise_for_status()  # Raise an exception for HTTP errors

        return response.text  # Return raw HTML content for BeautifulSoup

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
   


def parse_job_listings(html_content):
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all job listings
    job_listings = soup.find_all('li', class_='JobsList_jobListItem__wjTHv')

    jobs = []
    for job in job_listings:
        # Extract job title
        job_title_tag = job.find('a', class_='JobCard_jobTitle___7I6y')
        job_title = job_title_tag.get_text(strip=True) if job_title_tag else 'No job title found'

        # Extract company name
        company_name_tag = job.find('span', class_='EmployerProfile_compactEmployerName__LE242')
        company_name = company_name_tag.get_text(strip=True) if company_name_tag else 'No company name found'

        # Extract job location
        location_tag = job.find('div', class_='JobCard_location__rCz3x')
        location = location_tag.get_text(strip=True) if location_tag else 'No location found'

        # Extract job link
        job_link_tag = job.find('a', class_='JobCard_trackingLink__GrRYn')
        job_link = job_link_tag['href'] if job_link_tag else 'No job link found'
        # Ensure the link is complete
        job_link = f'https://www.glassdoor.com{job_link}' if job_link != 'No job link found' else job_link

        jobs.append({
            'title': job_title,
            'company': company_name,
            'location': location,
            'link': job_link
        })

    return jobs

class SearchHandler(UrlHandler):
    async def handle_job(self, job: Job) -> Optional[List[CrawlResult]]:
        if job.metadata.get("records_left", 0) <= 0:
            return None
        query=job.metadata['search_term']
        location_name=job.metadata['location']
        job.url = (
            f"https://www.glassdoor.com/Job/jobs.htm?"
            f"sc.keyword={query.replace(' ', '%20')}&"
            f"locT=C&locKeyword={location_name.replace(' ', '%20')}"
        )
        response = await self.url_resolver.get(
            job.url,
            referer=job.metadata.get("referer"),
            js_scenario=None,
        )
        #search_response = scrape_glassdoor(query=job.metadata['search_term'], location_name=job.metadata['location'])
         #crating an array for job listing 8/9/2024
        jobs_array = []

        # if search_response:
        #     job_listings = parse_job_listings(html_content=search_response)
        if response:
            job_listings = parse_job_listings(html_content=response)    

            for jobs in job_listings[:20]:  # Display the top 10 job listings
                
                print(f"Website: Glassdoor")
                print(f"Search Keyword: {job.metadata['search_term']}")
                print(f"Title: {jobs['title']}")
                print(f"Company: {jobs['company']}")
                print(f"Location: {jobs['location']}")
                print(f"Link: {jobs['link']}")
                print('-' * 80)
                jobs_array.append({
                'title': jobs['title'],
                'company': jobs['company'],
                'location': jobs['location'],
                'link': jobs['link']
                })
            
           
        else:
            print("No job listings found or an error occurred.")
        #check if these listings can be added directly to crawl results and sent to db table 8/9/2024
        # new_jobs = self.process_content(job_listings, job)
        # logger.debug(f"Adding {len(new_jobs)} new urls to queue")
        # await self.repository.post_job(new_jobs)
        return self.process_crawl_result(
                    job_listings,job=job
                )    # process
        #return None
    
    def process_crawl_result ( self, job_descriptions, job: Job
    ) -> Optional[List[CrawlResult]]:
        listofresults =[]
        compaddress = None
        placeid = None
        latitude = None
        longitude = None
        googleapikey = settings.GOOGLE_API_KEY
        

        for jobdesc in job_descriptions:
            location_data = google_places_handler.get_google_place_id(api_key=googleapikey,company_name=jobdesc['company'])
            if location_data is not None:
                placeid = location_data[0]
                latitude = location_data[1]
                longitude = location_data[2]
                compaddress = location_data[3]
            result = CrawlResult(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=jobdesc['link'],
                    company_name=jobdesc['company'],
                    address=compaddress,
                    position=jobdesc['title'],
                    description= "",
                    google_places_id=str(placeid),
                    # fixed exception 7/23/2024
                    lat = float(latitude) if latitude is not None else None,
                    long = float(longitude) if longitude is not None else None,
                    # added job_description 7/23/2024
                    job_description= ''
                )
            listofresults.append(result)
        return listofresults
        # return [
        #     CrawlResult(
        #         input_id=job.input_id,
        #         crawl_id=job.crawl_id,
        #         url=job_description['link'],
        #         company_name=job_description['company'],
        #         address=compaddress,
        #         position=job_description['title'],
        #         description= "",
        #         google_places_id=str(placeid),
        #         # fixed exception 7/23/2024
        #         lat = float(latitude) if latitude is not None else None,
        #         long = float(longitude) if longitude is not None else None,
        #         # added job_description 7/23/2024
        #         job_description= ''
        #     )
        # ]
        
        
    #changed process content to use job urls from json data 
    def process_content(self, Parsed_Job_list, job: Job) -> List[CreateJob]:
       # parsed_url = urlparse(response.headers.get("Resolved-Url"))
       #base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        jobs = []
        records_left = job.metadata.get("records_left", 0)
        for job_obj in Parsed_Job_list:
            if records_left <= 0:
                break
            new_metadata = copy.deepcopy(job.metadata)
            new_metadata["referer"] = job_obj['link']
            new_metadata["site_type"] = "details"

            jobs.append(
                CreateJob(
                    input_id=job.input_id,
                    crawl_id=job.crawl_id,
                    url=job_obj['link'],
                    metadata=new_metadata,
                )
            )
            records_left -= 1

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




# process details results as i have added jobs with detail pages in the previous search handler code 8/9/2024
class DetailsHandler(UrlHandler):
    async def handle_job(self, job: Job) -> Optional[List[CrawlResult]]:

        response = scrape_glassdoor(query=job.url ,location_name=job.metadata["location"])

        # response = await self.url_resolver.get(
        #     job.url, referer=job.metadata.get("referer"),
        #     #js_scenario=js_scenario
        # )
        return self.process_content(response, job)

    def process_content(
        self, response: httpx.Response, job: Job
    ) -> Optional[List[CrawlResult]]:
        soup = BeautifulSoup(response.content, "html.parser")
        company_link_tag = soup.find('a', class_='EmployerProfile_profileContainer')
        company_link = company_link_tag.get('href')
        if company_link is None:
            return None

        address_node = soup.select_one('div[data-test="location"]')
        address = None
        if address_node:
            address = address_node.text

        url = company_link
        name = company_link_tag.text.strip()

        # Not specifying the value of data-testid attribute as it's the only h1 there
        position_node = soup.find('h1', class_='heading_Level1')
        position = None
        if position_node:
            position = position_node.text.strip()

        #adding job description 7/19/2024
        job_description_node = soup.find('div', class_='JobDetails_jobDescription')

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
            
        # print(type(placeid))
        # print(type(latitude))
        # print(type(longitude))
        # print(type(compaddress))



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
