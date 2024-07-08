from datetime import date
from typing import Optional

from pydantic import BaseModel

from repository.models import CreateCrawl as RepoCreateCrawl

# from job_postings_crawler.repository.models import CreateCrawl as RepoCreateCrawl


class CreateCrawl(BaseModel):
    minimum_date: Optional[date]
    include_never_processed: bool = False

    def to_repo_model(self) -> RepoCreateCrawl:
        return RepoCreateCrawl(
            include_never_processed=self.include_never_processed,
            minimum_date=self.minimum_date,
        )
