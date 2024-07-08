from abc import ABC, abstractmethod
from typing import List, Optional

from repository.db_repository import QueueRepository
from repository.models import CrawlResult, Job
from url_resolver.base import UrlResolver

# from job_postings_crawler.repository.db_repository import QueueRepository
# from job_postings_crawler.repository.models import CrawlResult, Job
# from job_postings_crawler.url_resolver.base import UrlResolver


class UrlHandler(ABC):
    repository: QueueRepository

    def __init__(self, repository: QueueRepository, url_resolver: UrlResolver) -> None:
        self.repository = repository
        self.url_resolver = url_resolver

    @abstractmethod
    async def handle_job(self, job: Job) -> Optional[List[CrawlResult]]:
        pass
