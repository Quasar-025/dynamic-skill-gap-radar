"""
Scrapers for public job listing pages.

This module intentionally uses best-effort scraping and returns partial data when
some sources are blocked or rate-limited.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
import os
import re
from typing import Iterable, List, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class JobPosting:
    source: str
    title: str
    company: str
    location: str
    role: str
    description: str
    url: str
    scraped_at: str

    @property
    def uid(self) -> str:
        payload = f"{self.source}|{self.url}|{self.title}|{self.company}".lower().strip()
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class BaseScraper:
    source_name = "base"

    def __init__(self, timeout: int = 20):
        self.timeout = timeout
        self.blocked = False
        self.session = requests.Session()

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        raise NotImplementedError

    def _get(self, url: str) -> Optional[requests.Response]:
        try:
            response = self.session.get(url, headers=DEFAULT_HEADERS, timeout=self.timeout)
            if response.status_code != 200:
                if response.status_code in (401, 403, 429):
                    self.blocked = True
                logger.warning("%s returned status=%s for %s", self.source_name, response.status_code, url)
                return None
            return response
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("%s request failed for %s: %s", self.source_name, url, exc)
            return None


class IndeedScraper(BaseScraper):
    source_name = "indeed"

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        postings: List[JobPosting] = []
        encoded_query = quote_plus(query)
        encoded_location = quote_plus(location)

        for page in range(max_pages):
            start = page * 10
            url = f"https://www.indeed.com/jobs?q={encoded_query}&l={encoded_location}&start={start}"
            response = self._get(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, "lxml")
            cards = soup.select("div.job_seen_beacon")
            if not cards:
                cards = soup.select("a.tapItem")

            for card in cards:
                title_el = card.select_one("h2.jobTitle") or card.select_one("h2 a")
                company_el = card.select_one("span.companyName")
                location_el = card.select_one("div.companyLocation")
                snippet_el = card.select_one("div.job-snippet")
                link_el = card.select_one("a")

                title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
                company = company_el.get_text(" ", strip=True) if company_el else "Unknown"
                job_location = location_el.get_text(" ", strip=True) if location_el else location
                description = snippet_el.get_text(" ", strip=True) if snippet_el else ""

                href = str(link_el.get("href") or "") if link_el else ""
                if href and href.startswith("/"):
                    href = f"https://www.indeed.com{href}"

                postings.append(
                    JobPosting(
                        source=self.source_name,
                        title=title,
                        company=company,
                        location=job_location,
                        role=query,
                        description=description,
                        url=href or url,
                        scraped_at=datetime.now(timezone.utc).isoformat(),
                    )
                )

        return postings


class WellfoundScraper(BaseScraper):
    source_name = "wellfound"

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        postings: List[JobPosting] = []

        for page in range(1, max_pages + 1):
            url = (
                "https://wellfound.com/jobs?"
                f"query={quote_plus(query)}&location={quote_plus(location)}&page={page}"
            )
            response = self._get(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, "lxml")
            cards = soup.select("[data-test='StartupResult']")
            if not cards:
                cards = soup.select("div.styles_component__Qn4R2")

            for card in cards:
                title_el = card.select_one("a[data-test='JobTitle']") or card.select_one("h2 a")
                company_el = card.select_one("a[data-test='CompanyName']") or card.select_one("h3")
                location_el = card.select_one("span[data-test='Location']")
                snippet_el = card.select_one("p")

                title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
                company = company_el.get_text(" ", strip=True) if company_el else "Unknown"
                job_location = location_el.get_text(" ", strip=True) if location_el else location
                description = snippet_el.get_text(" ", strip=True) if snippet_el else ""

                href = str(title_el.get("href") or "") if title_el else ""
                if href and href.startswith("/"):
                    href = f"https://wellfound.com{href}"

                postings.append(
                    JobPosting(
                        source=self.source_name,
                        title=title,
                        company=company,
                        location=job_location,
                        role=query,
                        description=description,
                        url=href or url,
                        scraped_at=datetime.now(timezone.utc).isoformat(),
                    )
                )

        return postings


class LinkedInPublicScraper(BaseScraper):
    source_name = "linkedin"

    def __init__(self, timeout: int = 20):
        super().__init__(timeout=timeout)
        self.fetch_descriptions = os.getenv("LINKEDIN_FETCH_DESCRIPTIONS", "0") == "1"

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        postings: List[JobPosting] = []
        encoded_query = quote_plus(query)
        encoded_location = quote_plus(location)

        for page in range(max_pages):
            start = page * 25
            url = (
                "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
                f"keywords={encoded_query}&location={encoded_location}&start={start}"
            )
            response = self._get(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, "lxml")
            cards = soup.select("li")

            for card in cards:
                title_el = card.select_one("h3")
                company_el = card.select_one("h4")
                location_el = card.select_one("span.job-search-card__location")
                link_el = card.select_one("a.base-card__full-link")

                title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
                company = company_el.get_text(" ", strip=True) if company_el else "Unknown"
                job_location = location_el.get_text(" ", strip=True) if location_el else location
                href = str(link_el.get("href") or "") if link_el else ""

                description = ""
                if href and self.fetch_descriptions:
                    description = self._fetch_linkedin_description(href)

                postings.append(
                    JobPosting(
                        source=self.source_name,
                        title=title,
                        company=company,
                        location=job_location,
                        role=query,
                        description=description,
                        url=href or url,
                        scraped_at=datetime.now(timezone.utc).isoformat(),
                    )
                )

        return postings

    def _fetch_linkedin_description(self, url: str) -> str:
        response = self._get(url)
        if not response:
            return ""

        soup = BeautifulSoup(response.text, "lxml")
        desc_el = soup.select_one("div.show-more-less-html__markup")
        if not desc_el:
            return ""
        return re.sub(r"\s+", " ", desc_el.get_text(" ", strip=True))


class RemotiveApiScraper(BaseScraper):
    source_name = "remotive"

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        del location, max_pages
        url = f"https://remotive.com/api/remote-jobs?search={quote_plus(query)}"
        response = self._get(url)
        if not response:
            return []

        try:
            payload = response.json()
        except Exception:
            return []

        jobs = payload.get("jobs", [])
        postings: List[JobPosting] = []
        for job in jobs:
            postings.append(
                JobPosting(
                    source=self.source_name,
                    title=(job.get("title") or "Unknown").strip(),
                    company=(job.get("company_name") or "Unknown").strip(),
                    location=(job.get("candidate_required_location") or "Remote").strip(),
                    role=query,
                    description=re.sub(r"\s+", " ", job.get("description") or "").strip(),
                    url=(job.get("url") or "").strip(),
                    scraped_at=datetime.now(timezone.utc).isoformat(),
                )
            )

        return postings


class ArbeitnowApiScraper(BaseScraper):
    source_name = "arbeitnow"

    def scrape(self, query: str, location: str = "", max_pages: int = 1) -> List[JobPosting]:
        del location
        postings: List[JobPosting] = []

        query_lower = query.lower().strip()
        for page in range(1, max_pages + 1):
            url = f"https://www.arbeitnow.com/api/job-board-api?page={page}"
            response = self._get(url)
            if not response:
                continue

            try:
                payload = response.json()
            except Exception:
                continue

            jobs = payload.get("data", [])
            for job in jobs:
                title = (job.get("title") or "Unknown").strip()
                company = (job.get("company_name") or "Unknown").strip()
                description = re.sub(r"\s+", " ", job.get("description") or "").strip()
                haystack = f"{title} {description}".lower()

                # API does not offer direct role search; do local filtering.
                if query_lower and query_lower not in haystack:
                    continue

                postings.append(
                    JobPosting(
                        source=self.source_name,
                        title=title,
                        company=company,
                        location=(job.get("location") or "Remote").strip(),
                        role=query,
                        description=description,
                        url=(job.get("url") or "").strip(),
                        scraped_at=datetime.now(timezone.utc).isoformat(),
                    )
                )

        return postings


def get_default_scrapers() -> List[BaseScraper]:
    """
    Return source scrapers requested by product requirements.

    Each source can intermittently block traffic; callers should treat this as
    best-effort ingestion and continue with available source results.
    """
    return [
        RemotiveApiScraper(),
        ArbeitnowApiScraper(),
        IndeedScraper(),
        WellfoundScraper(),
        LinkedInPublicScraper(),
    ]


def dedupe_postings(postings: Iterable[JobPosting]) -> List[JobPosting]:
    seen = set()
    deduped: List[JobPosting] = []

    for posting in postings:
        if posting.uid in seen:
            continue
        seen.add(posting.uid)
        deduped.append(posting)

    return deduped
