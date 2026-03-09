# Dynamic Skill Gap Radar

Dynamic Skill Gap Radar analyzes live job-market demand and compares it with user resume skills.

## What This Project Does

- Periodically collects job postings from multiple public sources.
- Stores normalized postings and skill demand in Hive.
- Materializes parquet snapshots for fast API reads.
- Serves a dashboard to:
  - visualize current in-demand skills,
  - filter by role/company/region,
  - upload a resume,
  - run role-aware skill-gap analysis.

## Sources (Best Effort)

- Remotive API
- Arbeitnow API
- LinkedIn public job endpoint
- Indeed scraping (can be blocked)
- Wellfound scraping (can be blocked)

Blocked sources are auto-skipped for the rest of the current scrape cycle.

## Architecture

1. Scrape + Normalize: `spark_jobs/market_sync.py` and `spark_jobs/job_sources.py`
2. Persist to Hive: `spark_jobs/hive_store.py`
3. Snapshot parquet export:
   - `data/processed/market/job_postings.parquet`
   - `data/processed/market/skill_demand.parquet`
4. API + Dashboard: `dashboard/server.py` + `dashboard/index.html`

## Data Model

Hive database: `skill_gap`

Hive tables:
- `skill_gap.job_postings`
- `skill_gap.skill_demand`

## Quick Start

```bash
cd /home/quasar/repos/dynamic-skill-gap-radar
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

Terminal 1 (API):
```bash
source .venv/bin/activate
uvicorn dashboard.server:app --host 0.0.0.0 --port 8000 --reload
```

Terminal 2 (One-time ingest smoke test):
```bash
source .venv/bin/activate
export TARGET_ROLES="software development engineer,backend engineer,data engineer"
export TARGET_COMPANIES="amazon,microsoft,google"
export TARGET_LOCATION="United States"
export SCRAPE_MAX_PAGES=1
python -c "from spark_jobs.market_sync import scrape_once; print('rows_ingested=', scrape_once())"
```

Terminal 2 (Scheduler):
```bash
source .venv/bin/activate
export SCRAPE_INTERVAL_MINUTES=30
python spark_jobs/market_sync.py
```

Open dashboard at `http://localhost:8000`.

## Environment Variables

- `SCRAPE_INTERVAL_MINUTES`: scheduler interval (default: `30`)
- `TARGET_ROLES`: comma-separated role keywords
- `TARGET_COMPANIES`: comma-separated company keywords
- `TARGET_LOCATION`: location keyword for search
- `SCRAPE_MAX_PAGES`: pages requested per source/query
- `LINKEDIN_FETCH_DESCRIPTIONS`: `1` to fetch job detail pages (slower), default `0`

## APIs

- `GET /api/market_context`
- `GET /api/skill_demand?role=<role>&company=<company>&region=<region>&top_n=10`
- `GET /api/market_status`
- `POST /api/upload_resume`
- `POST /api/analyze_gap`

`/api/analyze_gap` uses fallback chain to avoid hard failure:
1. role+company+region snapshot
2. role+region snapshot
3. role+company snapshot
4. role-only snapshot
5. region-only snapshot
6. global snapshot
7. live websocket demand
8. baseline default demand

## Troubleshooting

- `UNSUPPORTED_OVERWRITE.TABLE`: fixed by staging table swap in Hive writes.
- Many `403/429` source logs: expected for some websites; pipeline continues with available sources.
- Empty chart initially: baseline demand is shown until snapshots become available.

## Git Hygiene

`.gitignore` excludes runtime-generated artifacts such as:
- Hive/Derby metastore files (`metastore_db/`, `derby.log`)
- Warehouse and snapshots (`data/hive/`, `spark-warehouse/`, `data/processed/`)
- Local virtualenv/env files (`.venv/`, `.env`)

## Legacy Components

Kafka/Spark streaming scripts still exist under `spark_jobs/` for optional experimentation, but the primary production flow is periodic scrape -> Hive -> snapshot -> API.
