import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin

import requests

from auth import FAFAuthClient

logger = logging.getLogger(__name__)


class FAFClient:
    def __init__(
        self,
        api_base_url: str,
        auth_client: FAFAuthClient,
        timeout: int = 30,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/") + "/"
        self.auth_client = auth_client
        self.timeout = timeout
        self.session = requests.Session()
        self._apply_auth_header()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def iter_pages(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
        start_page: int = 1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
        resume_file: Optional[Path] = None,
        resume_extra: Optional[Dict[str, Any]] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield one page of raw JSON-API records at a time.

        Pagination is done by manually incrementing page[number].
        Stops when a page returns 0 records.

        progress_callback(page_count, records_so_far, total_records_or_None)
          total_records is read from meta.page.totalRecords on the first page;
          None if the API doesn't provide it.
        """
        params = dict(params or {})
        page_count = 0
        total_fetched = 0
        total_records: Optional[int] = None
        page_number = start_page

        base_url = urljoin(self.api_base_url, endpoint.lstrip("/"))

        while True:
            params["page[number]"] = page_number
            logger.info("Fetching page %s (page[number]=%s)", page_count + 1, page_number)

            response = self._request_with_retry("GET", base_url, params=params)
            payload = response.json()
            records = payload.get("data", [])

            if not records:
                logger.info("Empty page received, download complete.")
                break

            # Extract total record count from meta on first page
            if total_records is None:
                try:
                    total_records = int(payload["meta"]["page"]["totalRecords"])
                    logger.info("Total records reported by API: %s", total_records)
                except (KeyError, TypeError, ValueError):
                    total_records = None

            total_fetched += len(records)
            page_count += 1

            if progress_callback:
                progress_callback(page_count, total_fetched, total_records)

            yield records

            if resume_file:
                self._save_resume_state(
                    resume_file, endpoint, page_number + 1, page_count,
                    extra=resume_extra,
                )

            if max_pages and page_count >= max_pages:
                logger.info("Reached max_pages limit (%s).", max_pages)
                break

            page_number += 1

        if resume_file and resume_file.exists() and not (max_pages and page_count >= max_pages):
            resume_file.unlink()

    def get_all_pages(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
        start_page: int = 1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
        resume_file: Optional[Path] = None,
        resume_extra: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        for page in self.iter_pages(
            endpoint,
            params=params,
            max_pages=max_pages,
            start_page=start_page,
            progress_callback=progress_callback,
            resume_file=resume_file,
            resume_extra=resume_extra,
        ):
            collected.extend(page)
        return collected

    def count_records(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """
        Return the total number of records matching params by making a single
        cheap probe request with page[size]=1 and reading meta.page.totalRecords.
        Returns None if the API doesn't provide the count.
        """
        probe_params = dict(params or {})
        probe_params["page[size]"] = 1
        probe_params["page[number]"] = 1

        base_url = urljoin(self.api_base_url, endpoint.lstrip("/"))
        try:
            response = self._request_with_retry("GET", base_url, params=probe_params)
            payload  = response.json()
            return int(payload["meta"]["page"]["totalRecords"])
        except (KeyError, TypeError, ValueError, Exception) as e:
            logger.warning("count_records probe failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Internal Helpers
    # ------------------------------------------------------------------

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        max_retries = 5
        for attempt in range(max_retries):
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)

            if response.status_code == 429:
                wait = 2 ** attempt
                logger.warning("Rate limited. Waiting %s seconds.", wait)
                time.sleep(wait)
                continue

            if response.status_code == 401:
                logger.info("Token expired. Refreshing.")
                self._refresh_auth()
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("Max retries exceeded.")

    def _apply_auth_header(self) -> None:
        token_data = self.auth_client.get_token()
        self.session.headers.update({"Authorization": f"Bearer {token_data['access_token']}"})

    def _refresh_auth(self) -> None:
        token_data = self.auth_client.get_token()
        self.session.headers.update({"Authorization": f"Bearer {token_data['access_token']}"})

    @staticmethod
    def _save_resume_state(
        resume_file: Path,
        endpoint: str,
        next_page: int,
        page_count: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        state: Dict[str, Any] = {
            "endpoint": endpoint,
            "next_page": next_page,
            "page_count": page_count,
        }
        if extra:
            state.update(extra)
        resume_file.write_text(json.dumps(state, indent=2), encoding="utf-8")