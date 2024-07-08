import json
from typing import Any, Callable, Dict

import httpx

from config import settings
from url_resolver.base import UrlResolver

# from job_postings_crawler.config import settings
# from job_postings_crawler.url_resolver.base import UrlResolver


def response_ok(response: httpx.Response) -> bool:
    return response.status_code == 404 or 200 <= response.status_code < 300


async def try_sf_n_times(
    params: httpx._types.QueryParamTypes,
    single_try_timeout_ms: int,
    base_url: str = settings.SF_API_URL,
    max_retries: int = 3,
    success: Callable = response_ok,
) -> httpx.Response:
    sf_response = None
    retry = 0
    last_err = ValueError("Scraping Fish response is None but no error was found!")
    async with httpx.AsyncClient() as client:
        while retry < max_retries:
            retry += 1
            try:
                sf_response = await client.get(
                    base_url, params=params, timeout=int(single_try_timeout_ms / 1000)
                )
            except Exception as e:
                last_err = e
                continue

            if success(sf_response):
                break

            if sf_response.status_code == 429:
                retry -= 1

    if sf_response is None:
        raise last_err

    return sf_response # API Response


class ScrapingFish(UrlResolver):
    # Time to wait after SF timeouts
    # Accommodates unpredictable network or OS conditions
    SF_TOTAL_TIMEOUT_LEEWAY_MS = 5_000

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    async def get(
        self,
        url: str,
        js_scenario: Dict[str, Any] | None = None,
        referer: str | None = None,
        render_js: bool = False,
        trial_timeout_ms: int = 30_000,
        total_timeout_ms: int = 90_000,
    ) -> httpx.Response:
        params = {
            "url": url,
            "api_key": self.api_key,
            "trial_timeout_ms": trial_timeout_ms,
            "total_timeout_ms": total_timeout_ms,
        }
        if js_scenario:
            params["js_scenario"] = json.dumps(js_scenario)
        if referer:
            params["referer"] = referer
        if render_js:
            params["render_js"] = "true"
        response = await try_sf_n_times(
            params=params,
            single_try_timeout_ms=total_timeout_ms + self.SF_TOTAL_TIMEOUT_LEEWAY_MS,
        )
        return response
