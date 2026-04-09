"""
ML microservice for skill extraction and normalization.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from functools import lru_cache
from typing import List, Dict

from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(title="Skill Gap ML Service", version="0.1.0")


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SPARK_JOBS_PATH = _PROJECT_ROOT / "spark_jobs"
if str(_SPARK_JOBS_PATH) not in sys.path:
    sys.path.append(str(_SPARK_JOBS_PATH))

from skill_extractor import TECH_SKILLS, extract_skills as regex_extract_skills  # noqa: E402  # pyright: ignore[reportMissingImports]


def _normalize_skill_name(skill: str) -> str:
    cleaned = (skill or "").strip()
    if not cleaned:
        return ""

    canonical = {
        "javascript": "JavaScript",
        "typescript": "TypeScript",
        "sql": "SQL",
        "aws": "AWS",
        "gcp": "GCP",
        "node.js": "Node.js",
        "next.js": "Next.js",
        "ci/cd": "CI/CD",
        "tensorflow": "TensorFlow",
        "pytorch": "PyTorch",
        "postgresql": "PostgreSQL",
        "mysql": "MySQL",
        "mongodb": "MongoDB",
        "scikit-learn": "Scikit-Learn",
        "fastapi": "FastAPI",
    }
    lowered = cleaned.lower()
    return canonical.get(lowered, cleaned.title())


SKILL_TAXONOMY = sorted({_normalize_skill_name(skill) for skill in TECH_SKILLS if skill})

SKILL_SYNONYMS = {
    "ml": "Machine Learning",
    "ai": "Machine Learning",
    "genai": "Machine Learning",
    "react.js": "React",
    "reactjs": "React",
    "js": "JavaScript",
    "ts": "TypeScript",
    "node": "Node.js",
    "tf": "TensorFlow",
    "sklearn": "Scikit-Learn",
    "k8s": "Kubernetes",
    "postgres": "PostgreSQL",
    "py": "Python",
}

MODEL_NAME = os.getenv("MODEL_NAME", "all-MiniLM-L6-v2")
ML_SCORE_THRESHOLD = float(os.getenv("ML_SCORE_THRESHOLD", "0.16"))
ML_TOP_K_MAX = int(os.getenv("ML_TOP_K_MAX", "120"))
ML_EXTRACTION_MODE = os.getenv("ML_EXTRACTION_MODE", "hybrid").lower()


class ExtractRequest(BaseModel):
    text: str
    top_k: int = 50
    min_score: float | None = None


class NormalizeRequest(BaseModel):
    skills: List[str]


@lru_cache(maxsize=1)
def _embedder():
    from sentence_transformers import SentenceTransformer  # pyright: ignore[reportMissingImports]

    return SentenceTransformer(MODEL_NAME)


@lru_cache(maxsize=1)
def _taxonomy_embeddings():
    model = _embedder()
    return model.encode(SKILL_TAXONOMY, normalize_embeddings=True)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_version": MODEL_NAME,
        "taxonomy_size": len(SKILL_TAXONOMY),
        "mode": ML_EXTRACTION_MODE,
        "threshold": ML_SCORE_THRESHOLD,
    }


@app.post("/normalize_skills")
def normalize_skills(request: NormalizeRequest):
    normalized = []
    for raw in request.skills:
        cleaned = (raw or "").strip()
        if not cleaned:
            continue

        key = cleaned.lower()
        canonical = SKILL_SYNONYMS.get(key, _normalize_skill_name(cleaned))
        normalized.append(canonical)

    # De-dupe while preserving order
    seen = set()
    unique = []
    for skill in normalized:
        if skill.lower() in seen:
            continue
        seen.add(skill.lower())
        unique.append(skill)

    return {"normalized": unique}


@app.post("/extract_skills")
def extract_skills(request: ExtractRequest):
    text = (request.text or "").strip()
    if not text:
        return {
            "skills": [],
            "model_version": MODEL_NAME,
            "taxonomy_size": len(SKILL_TAXONOMY),
            "mode": ML_EXTRACTION_MODE,
        }

    model = _embedder()
    text_vec = model.encode([text], normalize_embeddings=True)[0]
    taxonomy_vecs = _taxonomy_embeddings()

    # Cosine similarity with normalized vectors is just dot-product.
    scores = taxonomy_vecs @ text_vec

    ranked = sorted(
        [(SKILL_TAXONOMY[i], float(scores[i])) for i in range(len(SKILL_TAXONOMY))],
        key=lambda item: item[1],
        reverse=True,
    )

    top_k = max(1, min(request.top_k, min(len(ranked), ML_TOP_K_MAX)))
    threshold = ML_SCORE_THRESHOLD if request.min_score is None else float(request.min_score)

    selected: Dict[str, float] = {
        _normalize_skill_name(skill): round(score, 4)
        for skill, score in ranked[:top_k]
        if score >= threshold
    }

    if ML_EXTRACTION_MODE in {"hybrid", "ensemble"}:
        regex_hits = regex_extract_skills(text)
        for skill in regex_hits.keys():
            normalized = _normalize_skill_name(skill)
            selected[normalized] = max(selected.get(normalized, 0.0), threshold)

    selected_list = [
        {"name": skill, "score": round(score, 4)}
        for skill, score in sorted(selected.items(), key=lambda item: item[1], reverse=True)[:top_k]
    ]

    return {
        "skills": selected_list,
        "model_version": MODEL_NAME,
        "taxonomy_size": len(SKILL_TAXONOMY),
        "mode": ML_EXTRACTION_MODE,
        "threshold": threshold,
    }
