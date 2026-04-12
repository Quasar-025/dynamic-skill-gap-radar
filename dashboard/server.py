from fastapi import FastAPI, WebSocket, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import ast
from datetime import timedelta
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# Add spark_jobs directory to Python path
sys.path.append(str(Path(__file__).parent.parent / "spark_jobs"))

from resume_parser import parse_resume, clean_text
from skill_extractor import extract_skills_with_fallback
from skill_gap_analyzer import analyze_gap

app = FastAPI()

clients = []

# Store current skill demand data (updated by streaming processor)
current_skill_demand = {}

DEFAULT_SKILL_DEMAND = {
    "Python": 250,
    "SQL": 200,
    "AWS": 180,
    "Docker": 150,
    "Kubernetes": 120,
    "Java": 110,
    "React": 100,
    "Git": 90,
    "Machine Learning": 85,
    "TensorFlow": 70,
    "Spark": 65,
    "Kafka": 60,
}

# Pydantic models for request/response
class SkillGapRequest(BaseModel):
    resume_skills: List[str]
    role: Optional[str] = None
    company: Optional[str] = None
    region: Optional[str] = None
    top_n: int = 20


class JobRecommendationRequest(BaseModel):
    resume_skills: List[str]
    role: Optional[str] = None
    company: Optional[str] = None
    region: Optional[str] = None
    limit: int = 8
    max_age_days: int = 30

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def get_dashboard():
    """Serve the dashboard HTML"""
    html_path = Path(__file__).parent / "index.html"
    return FileResponse(html_path)

@app.get("/data")
async def get_spark_data():
    """Read latest demand data from scraped market snapshots"""
    demand = _load_market_demand(top_n=25)
    if not demand:
        demand = dict(list(DEFAULT_SKILL_DEMAND.items())[:25])
    return {
        "skills": [skill.title() for skill in demand.keys()],
        "counts": list(demand.values()),
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print(f"Client connected. Total clients: {len(clients)}")
    
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        print(f"Client disconnected: {e}")
    finally:
        if websocket in clients:
            clients.remove(websocket)
        print(f"Client removed. Total clients: {len(clients)}")

@app.post("/update")
async def update_dashboard(data: dict):
    """Receive updates and push to all connected clients"""
    print(f"Received update: {data}")
    print(f"Broadcasting to {len(clients)} clients")
    
    # Store the latest skill demand data
    global current_skill_demand
    if "skills" in data and "counts" in data:
        current_skill_demand = dict(zip(data["skills"], data["counts"]))
    
    disconnected = []
    for client in clients:
        try:
            await client.send_text(json.dumps(data))
        except Exception as e:
            print(f"Error sending to client: {e}")
            disconnected.append(client)
    
    # Remove disconnected clients
    for client in disconnected:
        if client in clients:
            clients.remove(client)
    
    return {"status": "sent", "clients": len(clients)}


@app.post("/api/upload_resume")
async def upload_resume(file: UploadFile = File(...)):
    """
    Upload a resume file and extract skills
    
    Accepts: PDF, DOCX, TXT files (max 5MB)
    Returns: Extracted text and identified skills
    """
    # Validate file size (5MB limit)
    MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB
    
    # Read file content
    content = await file.read()
    
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 5MB")
    
    # Validate file extension
    allowed_extensions = ['.pdf', '.docx', '.doc', '.txt']
    file_extension = Path(file.filename).suffix.lower()
    
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format. Allowed: {', '.join(allowed_extensions)}"
        )
    
    # Parse resume
    extracted_text = parse_resume(file.filename, content)
    
    if not extracted_text:
        raise HTTPException(status_code=500, detail="Failed to extract text from resume")
    
    # Clean text
    cleaned_text = clean_text(extracted_text)
    
    skills_dict, extraction_meta = _extract_resume_skills_full(cleaned_text)
    skills_list = sorted(skills_dict.keys())
    
    # Save resume (optional - for session)
    resume_dir = Path(__file__).parent.parent / "data" / "processed" / "resumes"
    resume_dir.mkdir(parents=True, exist_ok=True)
    
    return {
        "filename": file.filename,
        "text_length": len(cleaned_text),
        "skills_found": len(skills_list),
        "skills_detected_total": len(skills_list),
        "skills": skills_list,
        "skill_details": skills_dict,
        "preview": cleaned_text[:500],
        "extraction_meta": extraction_meta,
    }


@app.post("/api/analyze_gap")
async def analyze_skill_gap(request: SkillGapRequest):
    """
    Analyze skill gap between resume and current job market demand
    
    Args:
        request: Contains list of resume skills
        
    Returns:
        Detailed gap analysis with recommendations
    """
    if not request.resume_skills:
        raise HTTPException(status_code=400, detail="Resume skills list cannot be empty")
    
    market_demand, demand_source = _resolve_market_demand(
        role=request.role,
        company=request.company,
        region=request.region,
        top_n=request.top_n,
    )
    
    # Perform gap analysis
    analysis_result = analyze_gap(
        resume_skills=request.resume_skills,
        market_demand=market_demand,
        top_n_recommendations=max(5, min(request.top_n, 100)),
    )

    analysis_result["market_source"] = demand_source
    analysis_result["market_filters"] = {
        "role": request.role,
        "company": request.company,
        "region": request.region,
    }
    
    return analysis_result


@app.get("/api/market_context")
async def get_market_context(
    role: Optional[str] = Query(default=None),
    company: Optional[str] = Query(default=None),
    region: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Return available roles and companies from scraped postings."""
    context = _load_market_context(role=role, company=company, region=region, limit=limit)
    return context


@app.get("/api/top_jobs_by_region")
async def get_top_jobs_by_region(
    region: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=30),
):
    """Return top in-demand job titles/roles for a selected region."""
    try:
        import pandas as pd

        postings_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "job_postings.parquet"
        if not postings_path.exists():
            return {"region": region, "jobs": [], "total": 0, "source": "snapshot_missing"}

        df = pd.read_parquet(postings_path)
        if df.empty:
            return {"region": region, "jobs": [], "total": 0, "source": "snapshot_empty"}

        region_norm = region.lower().strip()
        if "location" not in df.columns:
            return {"region": region, "jobs": [], "total": 0, "source": "snapshot_missing_location"}

        region_df = df[df["location"].astype(str).str.lower().str.strip() == region_norm]
        if region_df.empty:
            return {"region": region, "jobs": [], "total": 0, "source": "snapshot_no_region_match"}

        title_col = "title" if "title" in region_df.columns else "role"
        grouped = (
            region_df[title_col]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", None)
            .dropna()
            .value_counts()
            .head(limit)
        )

        jobs = [{"name": str(title), "count": int(count)} for title, count in grouped.items()]
        return {
            "region": region,
            "jobs": jobs,
            "total": len(jobs),
            "source": "snapshot_region_jobs",
        }
    except Exception as e:
        print(f"Error loading top jobs by region: {e}")
        return {"region": region, "jobs": [], "total": 0, "source": "error"}


@app.get("/api/market_status")
async def get_market_status():
    """QoL endpoint with snapshot freshness and volume stats."""
    try:
        import pandas as pd

        postings_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "job_postings.parquet"
        if not postings_path.exists():
            return {
                "snapshot_ready": False,
                "total_jobs": 0,
                "total_roles": 0,
                "total_companies": 0,
                "total_regions": 0,
                "last_scraped_at": None,
            }

        df = pd.read_parquet(postings_path, columns=["source", "role", "company", "scraped_at"])
        if df.empty:
            return {
                "snapshot_ready": False,
                "total_jobs": 0,
                "total_roles": 0,
                "total_companies": 0,
                "total_regions": 0,
                "last_scraped_at": None,
            }

        source_counts = (
            df.groupby("source", as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=False)
        )

        return {
            "snapshot_ready": True,
            "total_jobs": int(len(df)),
            "total_roles": int(df["role"].dropna().nunique()),
            "total_companies": int(df["company"].dropna().nunique()),
            "total_regions": int(df["location"].dropna().nunique()) if "location" in df.columns else 0,
            "last_scraped_at": str(df["scraped_at"].dropna().max()) if df["scraped_at"].notna().any() else None,
            "sources": [
                {"source": str(row["source"]), "count": int(row["count"])}
                for _, row in source_counts.iterrows()
            ],
        }
    except Exception as e:
        print(f"Error loading market status: {e}")
        return {
            "snapshot_ready": False,
            "total_jobs": 0,
            "total_roles": 0,
            "total_companies": 0,
            "total_regions": 0,
            "last_scraped_at": None,
        }


@app.get("/api/skill_demand")
async def get_skill_demand(
    role: Optional[str] = Query(default=None),
    company: Optional[str] = Query(default=None),
    region: Optional[str] = Query(default=None),
    top_n: int = Query(default=25, ge=1, le=200),
):
    """
    Get current skill demand data from scraped market snapshots.
    """
    market_demand = _load_market_demand(role=role, company=company, region=region, top_n=top_n)

    if not market_demand and current_skill_demand:
        market_demand = current_skill_demand.copy()
        source = "live_stream"
    elif market_demand:
        source = "market_snapshot"
    else:
        market_demand = dict(list(DEFAULT_SKILL_DEMAND.items())[:top_n])
        source = "baseline_default"

    return {
        "skills": [skill.title() for skill in market_demand.keys()],
        "counts": list(market_demand.values()),
        "total_skills": len(market_demand),
        "source": source,
        "top_n": top_n,
    }


@app.post("/api/recommend_jobs")
async def recommend_jobs(request: JobRecommendationRequest):
    """
    Recommend best-matching jobs from current market postings for uploaded resume skills.
    """
    if not request.resume_skills:
        raise HTTPException(status_code=400, detail="Resume skills list cannot be empty")

    try:
        import pandas as pd

        postings_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "job_postings.parquet"
        if not postings_path.exists():
            return {
                "jobs": [],
                "total": 0,
                "filters": {
                    "role": request.role,
                    "company": request.company,
                    "region": request.region,
                },
                "source": "snapshot_missing",
            }

        df = pd.read_parquet(postings_path)
        if df.empty:
            return {
                "jobs": [],
                "total": 0,
                "filters": {
                    "role": request.role,
                    "company": request.company,
                    "region": request.region,
                },
                "source": "snapshot_empty",
            }

        if "scraped_at" in df.columns:
            ts = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
            latest = ts.max()
            max_age_days = max(1, min(int(request.max_age_days), 180))
            if pd.notna(latest):
                cutoff = latest - timedelta(days=max_age_days)
                df = df[ts >= cutoff]

        if request.role and "role" in df.columns:
            df = df[df["role"].astype(str).str.lower().str.strip() == request.role.lower().strip()]
        if request.company and "company" in df.columns:
            df = df[df["company"].astype(str).str.lower().str.strip() == request.company.lower().strip()]
        if request.region and "location" in df.columns:
            df = df[df["location"].astype(str).str.lower().str.strip() == request.region.lower().strip()]

        if df.empty:
            return {
                "jobs": [],
                "total": 0,
                "filters": {
                    "role": request.role,
                    "company": request.company,
                    "region": request.region,
                },
                "source": "snapshot_no_matches",
            }

        if "scraped_at" in df.columns:
            df = df.sort_values(by="scraped_at", ascending=False)

        candidate_df = df.head(800)
        resume_skill_set = {s.lower().strip() for s in request.resume_skills if str(s).strip()}
        if not resume_skill_set:
            raise HTTPException(status_code=400, detail="No valid resume skills found")

        market_demand, _ = _resolve_market_demand(
            role=request.role,
            company=request.company,
            region=request.region,
            top_n=100,
        )
        market_demand_norm = {k.lower(): int(v) for k, v in market_demand.items()}

        scored_jobs = []
        for _, row in candidate_df.iterrows():
            title = str(row.get("title") or "").strip()
            company = str(row.get("company") or "").strip()
            location = str(row.get("location") or "").strip()
            role = str(row.get("role") or "").strip()
            url = str(row.get("url") or "").strip()
            description = str(row.get("description") or "").strip()

            job_skills = _parse_skills_cell(row.get("skills"))
            if not job_skills:
                text_for_extraction = f"{title} {description}".strip()
                if text_for_extraction:
                    extracted = extract_skills_with_fallback(text_for_extraction)
                    job_skills = list(extracted.keys())

            fit_score, matched_skills, missing_skills = _score_job_for_resume(
                resume_skill_set=resume_skill_set,
                job_skills=job_skills,
            )

            if fit_score <= 0 or not matched_skills:
                continue

            demand_alignment = sum(market_demand_norm.get(skill.lower(), 0) for skill in matched_skills)

            scored_jobs.append(
                {
                    "title": title or "Unknown title",
                    "company": company or "Unknown company",
                    "location": location or "Unknown location",
                    "role": role,
                    "url": url,
                    "fit_score": fit_score,
                    "matched_skills": matched_skills,
                    "missing_skills": missing_skills[:8],
                    "matched_count": len(matched_skills),
                    "job_skill_count": len(job_skills),
                    "demand_alignment": demand_alignment,
                    "source": str(row.get("source") or ""),
                }
            )

        scored_jobs.sort(
            key=lambda job: (
                int(job["fit_score"]),
                int(job["matched_count"]),
                int(job["demand_alignment"]),
            ),
            reverse=True,
        )

        limit = max(1, min(int(request.limit), 20))
        return {
            "jobs": scored_jobs[:limit],
            "total": len(scored_jobs),
            "filters": {
                "role": request.role,
                "company": request.company,
                "region": request.region,
            },
            "source": "snapshot_ranked",
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error generating job recommendations: {e}")
        return {
            "jobs": [],
            "total": 0,
            "filters": {
                "role": request.role,
                "company": request.company,
                "region": request.region,
            },
            "source": "error",
        }


def _load_market_demand(
    role: Optional[str] = None,
    company: Optional[str] = None,
    region: Optional[str] = None,
    top_n: int = 20,
) -> Dict[str, int]:
    """Load role/company filtered demand from parquet snapshots produced by market_sync."""
    try:
        import pandas as pd

        parquet_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "skill_demand.parquet"
        if not parquet_path.exists():
            return {}

        df = pd.read_parquet(parquet_path)
        if df.empty:
            return {}

        if role:
            df = df[df["role_norm"].str.lower() == role.lower().strip()]
        if company:
            df = df[df["company_norm"].str.lower() == company.lower().strip()]
        if region and "location_norm" in df.columns:
            df = df[df["location_norm"].str.lower() == region.lower().strip()]

        if df.empty:
            return {}

        grouped = (
            df.groupby("skill", as_index=False)["demand_count"]
            .sum()
            .sort_values("demand_count", ascending=False)
            .head(top_n)
        )

        return {
            str(row["skill"]): int(row["demand_count"])
            for _, row in grouped.iterrows()
        }
    except Exception as e:
        print(f"Error loading market demand snapshot: {e}")
        return {}


def _parse_skills_cell(raw_skills: Any) -> List[str]:
    """Parse skills stored as list, JSON string, or comma-separated text."""
    if raw_skills is None:
        return []

    if isinstance(raw_skills, list):
        return [str(skill).strip() for skill in raw_skills if str(skill).strip()]

    if isinstance(raw_skills, tuple):
        return [str(skill).strip() for skill in raw_skills if str(skill).strip()]

    if isinstance(raw_skills, str):
        text = raw_skills.strip()
        if not text:
            return []

        # Try JSON or Python list literal first.
        if text.startswith("[") and text.endswith("]"):
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(text)
                    if isinstance(parsed, list):
                        return [str(skill).strip() for skill in parsed if str(skill).strip()]
                except Exception:
                    continue

        return [piece.strip() for piece in text.split(",") if piece.strip()]

    return []


def _score_job_for_resume(resume_skill_set: set[str], job_skills: List[str]) -> Tuple[int, List[str], List[str]]:
    """Compute job fit score and overlap details for resume skills vs job skills."""
    if not job_skills:
        return 0, [], []

    cleaned_job_skills = [str(skill).strip() for skill in job_skills if str(skill).strip()]
    if not cleaned_job_skills:
        return 0, [], []

    normalized_job = {skill.lower(): skill for skill in cleaned_job_skills}
    matched_lowers = sorted([skill for skill in normalized_job.keys() if skill in resume_skill_set])
    if not matched_lowers:
        return 0, [], sorted(cleaned_job_skills)

    matched_skills = [normalized_job[skill] for skill in matched_lowers]
    missing_skills = [
        original
        for lower, original in normalized_job.items()
        if lower not in resume_skill_set
    ]

    coverage_denominator = max(1, len(normalized_job))
    coverage_score = int(round((len(matched_skills) / coverage_denominator) * 100))

    # A small boost rewards absolute overlap so broad resumes rank better for multi-skill roles.
    overlap_boost = min(15, len(matched_skills) * 3)
    fit_score = min(100, coverage_score + overlap_boost)
    return fit_score, matched_skills, sorted(missing_skills)


def _resolve_market_demand(
    role: Optional[str],
    company: Optional[str],
    region: Optional[str],
    top_n: int,
) -> tuple[Dict[str, int], str]:
    """
    Demand fallback chain to avoid 503 for resume analysis:
    1) role + company
    2) role only
    3) global snapshot
    4) websocket live demand
    5) baseline static demand
    """
    demand = _load_market_demand(role=role, company=company, region=region, top_n=top_n)
    if demand:
        return demand, "snapshot_role_company_region"

    if role and company and region:
        demand = _load_market_demand(role=role, company=None, region=region, top_n=top_n)
        if demand:
            return demand, "snapshot_role_region"

    if role and company:
        demand = _load_market_demand(role=role, company=company, region=None, top_n=top_n)
        if demand:
            return demand, "snapshot_role_company"

    if role:
        demand = _load_market_demand(role=role, company=None, region=None, top_n=top_n)
        if demand:
            return demand, "snapshot_role_only"

    if region:
        demand = _load_market_demand(role=None, company=None, region=region, top_n=top_n)
        if demand:
            return demand, "snapshot_region_only"

    demand = _load_market_demand(role=None, company=None, region=None, top_n=top_n)
    if demand:
        return demand, "snapshot_global"

    if current_skill_demand:
        return current_skill_demand.copy(), "live_stream"

    return dict(list(DEFAULT_SKILL_DEMAND.items())[:top_n]), "baseline_default"


def _load_market_context(
    role: Optional[str] = None,
    company: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, List[str]]:
    try:
        import pandas as pd

        parquet_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "job_postings.parquet"
        if not parquet_path.exists():
            return {"roles": [], "companies": [], "regions": []}

        df = pd.read_parquet(parquet_path, columns=["role", "company", "location"])
        if df.empty:
            return {"roles": [], "companies": [], "regions": []}

        df["role"] = df["role"].astype(str).str.strip()
        df["company"] = df["company"].astype(str).str.strip()
        df["location"] = df["location"].astype(str).str.strip()

        if region:
            df = df[df["location"].str.lower() == region.lower().strip()]
        if role:
            df = df[df["role"].str.lower() == role.lower().strip()]
        if company:
            df = df[df["company"].str.lower() == company.lower().strip()]

        roles = sorted(
            [r for r in df["role"].dropna().astype(str).str.strip().unique().tolist() if r and r.lower() != "nan"]
        )[:limit]
        companies = sorted(
            [c for c in df["company"].dropna().astype(str).str.strip().unique().tolist() if c and c.lower() != "nan"]
        )[:limit]
        regions = sorted(
            [r for r in df["location"].dropna().astype(str).str.strip().unique().tolist() if r and r.lower() != "nan"]
        )[:limit]

        return {
            "roles": roles,
            "companies": companies,
            "regions": regions,
        }
    except Exception as e:
        print(f"Error loading market context snapshot: {e}")
        return {"roles": [], "companies": [], "regions": []}


def _extract_resume_skills_full(text: str) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """Extract skills from full resume text with chunking for long documents."""
    words = text.split()
    if not words:
        return {}, {
            "extraction_mode": "empty",
            "model_version": None,
            "used_fallback": False,
            "taxonomy_size": 0,
            "skills_detected_total": 0,
            "chunks_processed": 0,
        }

    chunk_size = 700
    overlap = 90
    start = 0
    merged: Dict[str, int] = {}
    modes = set()
    model_versions = set()
    used_fallback = False
    taxonomy_size = 0
    chunk_count = 0

    while start < len(words):
        end = min(len(words), start + chunk_size)
        chunk_text = " ".join(words[start:end])
        chunk_count += 1
        chunk_skills, chunk_meta = extract_skills_with_fallback(chunk_text, include_metadata=True)
        for skill_name, score in chunk_skills.items():
            merged[skill_name] = max(merged.get(skill_name, 0), int(score))

        modes.add(str(chunk_meta.get("extraction_mode", "unknown")))
        model_version = chunk_meta.get("model_version")
        if model_version:
            model_versions.add(str(model_version))
        used_fallback = used_fallback or bool(chunk_meta.get("used_fallback", False))
        taxonomy_size = max(taxonomy_size, int(chunk_meta.get("taxonomy_size") or 0))

        if end >= len(words):
            break
        start = max(0, end - overlap)

    meta = {
        "extraction_mode": "+".join(sorted(modes)) if modes else "unknown",
        "model_version": ",".join(sorted(model_versions)) if model_versions else None,
        "used_fallback": used_fallback,
        "taxonomy_size": taxonomy_size,
        "skills_detected_total": len(merged),
        "chunks_processed": chunk_count,
    }
    return merged, meta