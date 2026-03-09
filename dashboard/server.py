from fastapi import FastAPI, WebSocket, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import List, Dict

# Add spark_jobs directory to Python path
sys.path.append(str(Path(__file__).parent.parent / "spark_jobs"))

from resume_parser import parse_resume, clean_text
from skill_extractor import extract_skills, extract_skills_list
from skill_gap_analyzer import analyze_gap

app = FastAPI()

clients = []

# Store current skill demand data (updated by streaming processor)
current_skill_demand = {}

# Pydantic models for request/response
class SkillGapRequest(BaseModel):
    resume_skills: List[str]

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
    """Read processed data from Spark parquet files"""
    try:
        import pandas as pd
        parquet_path = Path(__file__).parent.parent / "data" / "processed" / "jobs_parquet"
        
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            
            # Example: Count skills (adjust based on your actual data structure)
            # For now, return sample data - you can customize this based on your parquet schema
            skills_data = {
                "skills": ["Python", "SQL", "Spark", "Machine Learning", "AWS"],
                "counts": [150, 120, 85, 95, 70]
            }
            return skills_data
        else:
            return {"skills": [], "counts": []}
    except Exception as e:
        print(f"Error reading data: {e}")
        return {"skills": [], "counts": []}

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
    
    # Use current market demand or load from parquet if not available
    market_demand = current_skill_demand.copy()
    
    if not market_demand:
        # Try to load from processed parquet data
        try:
            import pandas as pd
            parquet_path = Path(__file__).parent.parent / "data" / "processed" / "jobs_parquet"
            if parquet_path.exists():
                df = pd.read_parquet(parquet_path)
                # Try to extract skill data from parquet - adjust based on your schema
                # For now, use empty dict if no data available
                pass
        except Exception as e:
            print(f"Could not load market demand data: {e}")
    
    if not market_demand:
        # Use sample data as fallback
        market_demand = {
            "Python": 250, "SQL": 200, "AWS": 180, "Docker": 150,
            "Kubernetes": 120, "Java": 110, "React": 100, "Git": 90,
            "Machine Learning": 85, "TensorFlow": 70, "Spark": 65,
            "Kafka": 60, "Azure": 55, "MongoDB": 50
        }
    
    # Perform gap analysis
    analysis_result = analyze_gap(
        resume_skills=request.resume_skills,
        market_demand=market_demand,
        top_n_recommendations=10
    )
    
    return analysis_result


@app.get("/api/skill_demand")
async def get_skill_demand():
    """
    Get current skill demand data from job market
    
    Returns:
        Dictionary of skills and their demand counts
    """
    if current_skill_demand:
        return {
            "skills": list(current_skill_demand.keys()),
            "counts": list(current_skill_demand.values()),
            "total_skills": len(current_skill_demand),
            "source": "live_stream"
        }
    
    # Try to load from parquet if no live data
    try:
        import pandas as pd
        parquet_path = Path(__file__).parent.parent / "data" / "processed" / "jobs_parquet"
        
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            # Return sample data structure
            return {
                "skills": ["Python", "SQL", "Spark", "Machine Learning", "AWS"],
                "counts": [150, 120, 85, 95, 70],
                "total_skills": 5,
                "source": "parquet"
            }
    except Exception as e:
        print(f"Error loading data: {e}")
    
    # Return empty result if no data available
    return {
        "skills": [],
        "counts": [],
        "total_skills": 0,
        "source": "none"
    }