# Live Market Pipeline (Scrape -> Hive -> Dashboard)

This project now supports periodic market ingestion from:
- Remotive API
- Arbeitnow API
- Indeed
- Wellfound
- LinkedIn public job pages

The ingestion is best-effort. Some sources may intermittently block requests.

## 1. Install dependencies

```bash
cd /home/quasar/repos/dynamic-skill-gap-radar
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## 2. Start the dashboard API

```bash
source .venv/bin/activate
uvicorn dashboard.server:app --host 0.0.0.0 --port 8000 --reload
```

## 3. Start periodic market sync job

```bash
source .venv/bin/activate
export SCRAPE_INTERVAL_MINUTES=30
export TARGET_ROLES="software development engineer,backend engineer,data engineer"
export TARGET_COMPANIES="amazon,microsoft,google"
export TARGET_LOCATION="United States"
export SCRAPE_MAX_PAGES=1
python spark_jobs/market_sync.py
```

## 4. Open dashboard

Visit `http://localhost:8000`.

You can now:
- Select role, company, and region filters (for example, role=`software development engineer`, company=`amazon`, region=`United States`)
- View filtered top in-demand skills
- Upload resume and run gap analysis against the selected market segment

## Data outputs

The sync process writes:
- Hive database: `skill_gap`
- Hive table: `skill_gap.job_postings`
- Hive table: `skill_gap.skill_demand`
- Snapshot parquet: `data/processed/market/job_postings.parquet`
- Snapshot parquet: `data/processed/market/skill_demand.parquet`

## API endpoints

- `GET /api/market_context`
- `GET /api/skill_demand?role=<role>&company=<company>&region=<region>&top_n=10`
- `GET /api/market_status`
- `POST /api/analyze_gap`
  - body fields: `resume_skills`, `role`, `company`, `top_n`

## Notes

- Sources can be rate-limited or blocked; blocked sources are auto-skipped per cycle.
- Gap analysis now has fallback behavior (role/company -> role-only -> global -> live -> baseline), so it does not fail when a narrow filter has sparse data.
- Keep the old Kafka streaming flow only if you still want additional real-time pushes from other sources.
