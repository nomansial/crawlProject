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
