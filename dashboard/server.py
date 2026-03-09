from fastapi import FastAPI, WebSocket, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import sys
from pathlib import Path
from typing import List, Dict, Optional

# Add spark_jobs directory to Python path
sys.path.append(str(Path(__file__).parent.parent / "spark_jobs"))

from resume_parser import parse_resume, clean_text
from skill_extractor import extract_skills
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
    demand = _load_market_demand(top_n=10)
    if not demand:
        demand = dict(list(DEFAULT_SKILL_DEMAND.items())[:10])
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
    
    # Extract skills
    skills_dict = extract_skills(cleaned_text)
    skills_list = list(skills_dict.keys())
    
    # Save resume (optional - for session)
    resume_dir = Path(__file__).parent.parent / "data" / "processed" / "resumes"
    resume_dir.mkdir(parents=True, exist_ok=True)
    
    return {
        "filename": file.filename,
        "text_length": len(cleaned_text),
        "skills_found": len(skills_list),
        "skills": skills_list,
        "skill_details": skills_dict,
        "preview": cleaned_text[:500]  # First 500 characters as preview
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
        top_n_recommendations=10
    )

    analysis_result["market_source"] = demand_source
    analysis_result["market_filters"] = {
        "role": request.role,
        "company": request.company,
        "region": request.region,
    }
    
    return analysis_result


@app.get("/api/market_context")
async def get_market_context(limit: int = Query(default=200, ge=1, le=1000)):
    """Return available roles and companies from scraped postings."""
    context = _load_market_context(limit=limit)
    return context


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
    top_n: int = Query(default=10, ge=1, le=50),
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


def _load_market_context(limit: int = 200) -> Dict[str, List[str]]:
    try:
        import pandas as pd

        parquet_path = Path(__file__).parent.parent / "data" / "processed" / "market" / "job_postings.parquet"
        if not parquet_path.exists():
            return {"roles": [], "companies": []}

        df = pd.read_parquet(parquet_path, columns=["role", "company", "location"])
        if df.empty:
            return {"roles": [], "companies": [], "regions": []}

        roles = sorted(
            [r for r in df["role"].dropna().astype(str).str.strip().unique().tolist() if r]
        )[:limit]
        companies = sorted(
            [c for c in df["company"].dropna().astype(str).str.strip().unique().tolist() if c]
        )[:limit]
        regions = sorted(
            [r for r in df["location"].dropna().astype(str).str.strip().unique().tolist() if r]
        )[:limit]

        return {
            "roles": roles,
            "companies": companies,
            "regions": regions,
        }
    except Exception as e:
        print(f"Error loading market context snapshot: {e}")
        return {"roles": [], "companies": [], "regions": []}