from abc import ABC, abstractmethod
from typing import Any, Dict

import httpx


class UrlResolver(ABC):
    @abstractmethod
    async def get(
        self,
        url: str,
        js_scenario: Dict[str, Any] | None = None,
        referer: str | None = None,
        trial_timeout_ms: int = 30_000,
        total_timeout_ms: int = 90_000,
    ) -> httpx.Response:
        pass
