"""
Periodic scraper -> Hive sync job.

Run this long-lived process in its own terminal:
    python spark_jobs/market_sync.py
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, List

import requests

# Support both execution styles:
# 1) python -c "from spark_jobs.market_sync import scrape_once"
# 2) python spark_jobs/market_sync.py
try:
    from spark_jobs.hive_store import build_hive_spark, upsert_postings, fetch_market_demand
    from spark_jobs.job_sources import dedupe_postings, get_default_scrapers
    from spark_jobs.skill_extractor import extract_skills_list
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).parent))
    from hive_store import build_hive_spark, upsert_postings, fetch_market_demand
    from job_sources import dedupe_postings, get_default_scrapers
    from skill_extractor import extract_skills_list


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _env_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name, "")
    values = [v.strip() for v in raw.split(",") if v.strip()]
    return values or default


def _build_search_queries(roles: List[str], companies: List[str]) -> List[str]:
    queries: List[str] = []

    for role in roles:
        queries.append(role)
        for company in companies:
            queries.append(f"{role} {company}")

    # Preserve order while removing duplicates
    seen = set()
    unique_queries = []
    for query in queries:
        key = query.lower().strip()
        if key in seen:
            continue
        seen.add(key)
        unique_queries.append(query)

    return unique_queries


def scrape_once() -> int:
    roles = _env_list(
        "TARGET_ROLES",
        [
            "software development engineer",
            "backend engineer",
            "data engineer",
            "machine learning engineer",
        ],
    )
    companies = _env_list(
        "TARGET_COMPANIES",
        ["amazon", "microsoft", "google", "meta", "netflix"],
    )
    location = os.getenv("TARGET_LOCATION", "United States")
    max_pages = int(os.getenv("SCRAPE_MAX_PAGES", "1"))

    queries = _build_search_queries(roles, companies)
    scrapers = get_default_scrapers()

    all_postings = []
    for query in queries:
        for scraper in scrapers:
            if getattr(scraper, "blocked", False):
                logger.info("source=%s skipped (temporarily blocked)", scraper.source_name)
                continue
            try:
                result = scraper.scrape(query=query, location=location, max_pages=max_pages)
                logger.info("source=%s query='%s' count=%d", scraper.source_name, query, len(result))
                all_postings.extend(result)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("source=%s query='%s' failed: %s", scraper.source_name, query, exc)

    deduped = dedupe_postings(all_postings)

    rows = []
    for posting in deduped:
        text = f"{posting.title} {posting.description}"
        rows.append(
            {
                "job_uid": posting.uid,
                "source": posting.source,
                "title": posting.title,
                "company": posting.company,
                "location": posting.location,
                "role": posting.role,
                "description": posting.description,
                "url": posting.url,
                "scraped_at": posting.scraped_at,
                "skills": extract_skills_list(text),
            }
        )

    spark = build_hive_spark(app_name="SkillGapMarketSync")
    try:
        total_rows = upsert_postings(spark, rows)
        logger.info("Hive sync complete. total_rows=%d incoming=%d", total_rows, len(rows))

        # Push best-effort chart update for the default view.
        top_demand = fetch_market_demand(spark, role=roles[0], company=None, top_n=10)
        if top_demand:
            payload = {
                "skills": [skill.title() for skill in top_demand.keys()],
                "counts": list(top_demand.values()),
            }
            _push_update(payload)

        return len(rows)
    finally:
        spark.stop()
        _restore_default_sigint_handler()


def _restore_default_sigint_handler() -> None:
    """PySpark installs a SIGINT handler tied to SparkContext; reset it after stop()."""
    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
    except Exception:
        pass


def _push_update(payload: Dict) -> None:
    try:
        response = requests.post("http://localhost:8000/update", json=payload, timeout=5)
        logger.info("Pushed dashboard update status=%s", response.status_code)
    except Exception as exc:  # pragma: no cover - network dependent
        logger.warning("Could not push dashboard update: %s", exc)


def run_scheduler() -> None:
    interval_minutes = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "30"))
    interval_seconds = max(60, interval_minutes * 60)

    logger.info("Starting scheduler interval=%s minutes", interval_minutes)
    try:
        while True:
            try:
                scrape_once()
            except Exception as exc:
                logger.exception("Scrape cycle failed: %s", exc)

            logger.info("Sleeping for %d seconds", interval_seconds)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")


if __name__ == "__main__":
    run_scheduler()
