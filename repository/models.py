import datetime
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class CreateJob:
    url: str
    input_id: int
    crawl_id: int
    metadata: Dict[str, Any]
    status: str | None = None
    last_error: str | None = None
    attempt: int = 0
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    id: int | None = None


@dataclass
class Job(CreateJob):
    status: str
    attempt: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: int


@dataclass
class CreateCrawl:
    include_never_processed: bool
    status: str | None = None
    minimum_date: datetime.date | None = None
    created_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    id: int | None = None


@dataclass
class Crawl(CreateCrawl):
    status: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: int


@dataclass
class InputRow:
    scraping_id: int
    website: str
    search_term: str
    record_limit: int
    gl: str
    uule: str


@dataclass
class CrawlResult:
    input_id: int
    crawl_id: int
    url: str | None = None
    company_name: str | None = None
    address: str | None = None
    phone_number: str | None = None
    google_places_id: str | None = None
    position: str | None = None
    #adding job description here 
    description: str | None = None
    #update 7/20/2024 added latitude , longitude
    lat:float |None=None
    long:float|None=None
    # Added property for Job Description 7/23/2024
    job_description: str| None = None
    #added property for record count 7/31/2024
    record_count: int| None = None
    #added property for duplicate records with cache table 7/31/2024
    duplicate_record_count: int |None = None
    #added search site for source and search term for term 8/3/2024
    search_site : str |None= None
    search_term : str |None = None



    
