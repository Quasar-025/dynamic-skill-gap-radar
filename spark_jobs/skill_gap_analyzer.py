"""
Skill Gap Analyzer - Compare resume skills with job market demand
"""
from typing import Dict, List, Tuple
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def analyze_gap(
    resume_skills: List[str],
    market_demand: Dict[str, int],
    top_n_recommendations: int = 20
) -> Dict:
    """
    Analyze skill gap between resume and market demand
    
    Args:
        resume_skills: List of skills found in resume
        market_demand: Dictionary of skill name to demand count
        top_n_recommendations: Number of recommendations to generate
        
    Returns:
        Dictionary containing analysis results:
        - matching_skills: Skills present in both resume and market
        - missing_skills: High-demand skills missing from resume
        - skill_score: Overall match score (0-100)
        - recommendations: Prioritized list of skills to learn
        - resume_skill_count: Total skills in resume
        - market_skill_count: Total skills in demand
    """
    
    # Normalize skill names for comparison (case-insensitive)
    resume_skills_normalized = {skill.lower(): skill for skill in resume_skills}
    market_demand_normalized = {skill.lower(): count for skill, count in market_demand.items()}
    
    # Find matching skills
    matching_skills = []
    for skill_lower, original_skill in resume_skills_normalized.items():
        if skill_lower in market_demand_normalized:
            matching_skills.append({
                "skill": original_skill,
                "demand_count": market_demand_normalized[skill_lower]
            })
    
    # Sort matching skills by demand
    matching_skills.sort(key=lambda x: x["demand_count"], reverse=True)
    
    # Find missing high-demand skills
    missing_skills = []
    for skill_lower, count in market_demand_normalized.items():
        if skill_lower not in resume_skills_normalized:
            # Find original case from market_demand
            original_skill = next(
                (k for k in market_demand.keys() if k.lower() == skill_lower),
                skill_lower.title()
            )
            missing_skills.append({
                "skill": original_skill,
                "demand_count": count
            })
    
    # Sort missing skills by demand (highest first)
    missing_skills.sort(key=lambda x: x["demand_count"], reverse=True)
    
    # Calculate skill match score (0-100)
    if market_demand:
        # Weight by demand counts
        total_demand = sum(market_demand_normalized.values())
        matched_demand = sum(
            market_demand_normalized[s.lower()] 
            for s in resume_skills_normalized.keys() 
            if s.lower() in market_demand_normalized
        )
        skill_score = int((matched_demand / total_demand) * 100) if total_demand > 0 else 0
    else:
        skill_score = 0
    
    # Generate recommendations
    recommendations = generate_recommendations(
        matching_skills,
        missing_skills,
        top_n=top_n_recommendations
    )
    
    missing_limit = max(20, top_n_recommendations * 2)

    return {
        "matching_skills": matching_skills,
        "missing_skills": missing_skills[:missing_limit],
        "skill_score": skill_score,
        "recommendations": recommendations,
        "resume_skill_count": len(resume_skills),
        "market_skill_count": len(market_demand),
        "match_count": len(matching_skills),
        "gap_count": len(missing_skills)
    }


def generate_recommendations(
    matching_skills: List[Dict],
    missing_skills: List[Dict],
    top_n: int = 10
) -> List[Dict]:
    """
    Generate prioritized skill recommendations
    
    Args:
        matching_skills: List of skills already possessed
        missing_skills: List of missing skills with demand counts
        top_n: Number of recommendations to return
        
    Returns:
        List of recommendation dictionaries with skill, priority, and reason
    """
    recommendations = []
    
    # Prioritize missing skills by demand
    for i, skill_info in enumerate(missing_skills[:top_n]):
        priority = "High" if i < 5 else "Medium"
        
        # Generate contextual reason
        demand_count = skill_info["demand_count"]
        skill = skill_info["skill"]
        
        if demand_count > 100:
            reason = f"Extremely high demand ({demand_count}+ job postings)"
        elif demand_count > 50:
            reason = f"Very high demand ({demand_count} job postings)"
        elif demand_count > 20:
            reason = f"High demand ({demand_count} job postings)"
        else:
            reason = f"Moderate demand ({demand_count} job postings)"
        
        # Add category-specific suggestions
        category = categorize_skill(skill)
        if category:
            reason += f" - {category}"
        
        recommendations.append({
            "skill": skill,
            "priority": priority,
            "reason": reason,
            "demand_count": demand_count,
            "rank": i + 1
        })
    
    return recommendations


def categorize_skill(skill: str) -> str:
    """
    Categorize a skill to provide better context
    
    Args:
        skill: Skill name
        
    Returns:
        Category description or empty string
    """
    skill_lower = skill.lower()
    
    # Programming languages
    prog_langs = ["python", "java", "javascript", "c++", "c#", "go", "rust", "ruby", "php"]
    if any(lang in skill_lower for lang in prog_langs):
        return "Programming Language"
    
    # Cloud platforms
    cloud = ["aws", "azure", "gcp", "google cloud", "cloud"]
    if any(c in skill_lower for c in cloud):
        return "Cloud Platform"
    
    # Databases
    databases = ["sql", "mongodb", "postgresql", "mysql", "redis", "cassandra"]
    if any(db in skill_lower for db in databases):
        return "Database Technology"
    
    # ML/AI
    ml_ai = ["machine learning", "deep learning", "tensorflow", "pytorch", "ai"]
    if any(ml in skill_lower for ml in ml_ai):
        return "AI/Machine Learning"
    
    # DevOps
    devops = ["docker", "kubernetes", "jenkins", "terraform", "ansible"]
    if any(do in skill_lower for do in devops):
        return "DevOps/Infrastructure"
    
    # Web frameworks
    web = ["react", "angular", "vue", "django", "flask", "node"]
    if any(w in skill_lower for w in web):
        return "Web Framework"
    
    # Data Engineering
    data_eng = ["spark", "kafka", "airflow", "hadoop", "hive"]
    if any(de in skill_lower for de in data_eng):
        return "Data Engineering"
    
    return ""


def calculate_category_gaps(
    resume_skills: List[str],
    market_demand: Dict[str, int]
) -> Dict[str, Dict]:
    """
    Calculate skill gaps by category
    
    Args:
        resume_skills: List of skills from resume
        market_demand: Market skill demand
        
    Returns:
        Dictionary of categories with gap analysis
    """
    categories = {
        "Programming Languages": [],
        "Cloud Platforms": [],
        "Databases": [],
        "AI/ML": [],
        "DevOps": [],
        "Web Frameworks": [],
        "Data Engineering": [],
        "Other": []
    }
    
    resume_skills_lower = [s.lower() for s in resume_skills]
    
    for skill, count in market_demand.items():
        category = categorize_skill(skill) or "Other"
        
        has_skill = skill.lower() in resume_skills_lower
        
        if category in categories:
            categories[category].append({
                "skill": skill,
                "has_skill": has_skill,
                "demand": count
            })
    
    # Calculate summary for each category
    category_summary = {}
    for category, skills in categories.items():
        if skills:
            total = len(skills)
            has = sum(1 for s in skills if s["has_skill"])
            missing = total - has
            
            category_summary[category] = {
                "total_skills": total,
                "has_skills": has,
                "missing_skills": missing,
                "coverage_percentage": int((has / total) * 100) if total > 0 else 0,
                "top_missing": [
                    s["skill"] for s in sorted(skills, key=lambda x: x["demand"], reverse=True)
                    if not s["has_skill"]
                ][:3]
            }
    
    return category_summary


if __name__ == "__main__":
    # Test the analyzer
    resume_skills = ["Python", "SQL", "Git", "Linux", "Pandas", "NumPy"]
    
    market_demand = {
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
        "Kafka": 60
    }
    
    print("=== Skill Gap Analysis Test ===\n")
    print(f"Resume Skills: {resume_skills}")
    print(f"\nMarket Demand: {len(market_demand)} skills tracked\n")
    
    result = analyze_gap(resume_skills, market_demand, top_n_recommendations=8)
    
    print(f"Skill Match Score: {result['skill_score']}/100")
    print(f"Matching Skills: {result['match_count']}")
    print(f"Missing Skills: {result['gap_count']}\n")
    
    print("=== Matching Skills ===")
    for skill_info in result["matching_skills"][:5]:
        print(f"  ✓ {skill_info['skill']} (demand: {skill_info['demand_count']})")
    
    print("\n=== Top Missing Skills ===")
    for skill_info in result["missing_skills"][:5]:
        print(f"  ✗ {skill_info['skill']} (demand: {skill_info['demand_count']})")
    
    print("\n=== Recommendations ===")
    for rec in result["recommendations"][:5]:
        print(f"  {rec['rank']}. [{rec['priority']}] {rec['skill']}")
        print(f"     {rec['reason']}")
