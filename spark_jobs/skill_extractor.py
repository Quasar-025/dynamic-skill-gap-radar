"""
Skill Extractor - Extract and identify technical skills from text
"""
import re
from typing import List, Dict, Set
from collections import Counter

# Comprehensive skill dictionary covering various technical domains
TECH_SKILLS = [
    # Programming Languages
    "python", "java", "javascript", "c++", "c#", "go", "rust", "ruby", "php", 
    "swift", "kotlin", "typescript", "scala", "r", "matlab", "perl", "shell",
    "bash", "powershell", "objective-c", "dart", "elixir", "clojure", "haskell",
    
    # Web Frameworks & Libraries
    "react", "angular", "vue", "vue.js", "node.js", "express", "flask", "django",
    "spring", "spring boot", "asp.net", ".net", "fastapi", "nextjs", "next.js",
    "svelte", "ember", "backbone", "jquery", "bootstrap", "tailwind", "sass",
    
    # Mobile Development
    "android", "ios", "react native", "flutter", "xamarin", "ionic",
    
    # Databases
    "sql", "mysql", "postgresql", "oracle", "mongodb", "redis", "cassandra",
    "dynamodb", "elasticsearch", "neo4j", "mariadb", "sqlite", "couchdb",
    "influxdb", "snowflake", "bigquery", "redshift",
    
    # Cloud Platforms
    "aws", "azure", "gcp", "google cloud", "heroku", "digitalocean", "ibm cloud",
    "oracle cloud", "alibaba cloud",
    
    # Cloud Services
    "ec2", "s3", "lambda", "cloudformation", "cloudwatch", "ecs", "eks",
    "azure devops", "cloud functions", "cloud run", "app engine",
    
    # DevOps & CI/CD
    "docker", "kubernetes", "jenkins", "gitlab", "gitlab ci", "github actions",
    "circleci", "travis ci", "terraform", "ansible", "puppet", "chef",
    "vagrant", "helm", "prometheus", "grafana", "datadog", "new relic",
    "splunk", "nagios",
    
    # Data Engineering & Big Data
    "spark", "hadoop", "hive", "pig", "kafka", "airflow", "flink", "storm",
    "nifi", "talend", "informatica", "databricks", "presto", "dbt",
    
    # ML/AI & Data Science
    "machine learning", "deep learning", "tensorflow", "pytorch", "keras",
    "scikit-learn", "pandas", "numpy", "scipy", "matplotlib", "seaborn",
    "plotly", "opencv", "nltk", "spacy", "transformers", "hugging face",
    "xgboost", "lightgbm", "catboost", "jupyter", "anaconda",
    
    # Data Visualization & BI
    "tableau", "power bi", "looker", "qlik", "d3.js", "superset",
    
    # Version Control & Collaboration
    "git", "github", "gitlab", "bitbucket", "svn", "mercurial", "jira",
    "confluence", "slack", "trello",
    
    # Testing & QA
    "selenium", "junit", "pytest", "jest", "mocha", "cypress", "testng",
    "cucumber", "postman", "swagger", "rest assured",
    
    # Operating Systems & Tools
    "linux", "unix", "ubuntu", "centos", "redhat", "windows server", "macos",
    "vim", "emacs", "vscode", "intellij", "eclipse", "pycharm",
    
    # Web Technologies & APIs
    "rest", "restful", "graphql", "soap", "grpc", "websocket", "api",
    "microservices", "oauth", "jwt", "http", "https", "json", "xml",
    
    # Networking & Security
    "tcp/ip", "dns", "ssl", "tls", "vpn", "firewall", "encryption",
    "penetration testing", "security", "cybersecurity", "owasp",
    
    # Methodologies & Practices
    "agile", "scrum", "kanban", "devops", "ci/cd", "tdd", "bdd",
    "object oriented", "oop", "functional programming", "design patterns",
    "microservices", "serverless", "event driven",
    
    # Other Tools & Technologies
    "webpack", "babel", "npm", "yarn", "maven", "gradle", "git", "redis",
    "rabbitmq", "nginx", "apache", "tomcat", "iis", "load balancing"
]

# Create a set for faster lookups
TECH_SKILLS_SET = set(skill.lower() for skill in TECH_SKILLS)


def extract_skills(text: str, use_advanced: bool = False) -> Dict[str, int]:
    """
    Extract technical skills from text
    
    Args:
        text: Input text (resume, job description, etc.)
        use_advanced: Whether to use advanced NLP methods (requires spaCy)
        
    Returns:
        Dictionary mapping skill name to count/score
    """
    if not text:
        return {}
    
    text_lower = text.lower()
    
    # Basic pattern matching approach
    skill_counts = {}
    
    for skill in TECH_SKILLS:
        skill_lower = skill.lower()
        
        # Use word boundary regex for more accurate matching
        # This prevents matching "java" in "javascript"
        pattern = r'\b' + re.escape(skill_lower) + r'\b'
        matches = re.findall(pattern, text_lower)
        
        if matches:
            # Normalize skill name to title case for display
            normalized_skill = skill.title()
            skill_counts[normalized_skill] = len(matches)
    
    return skill_counts


def extract_skills_list(text: str) -> List[str]:
    """
    Extract list of unique skills found in text
    
    Args:
        text: Input text
        
    Returns:
        Sorted list of skill names
    """
    skill_counts = extract_skills(text)
    return sorted(skill_counts.keys())


def extract_top_skills(text: str, top_n: int = 10) -> Dict[str, int]:
    """
    Extract top N skills by frequency
    
    Args:
        text: Input text
        top_n: Number of top skills to return
        
    Returns:
        Dictionary of top N skills with counts
    """
    skill_counts = extract_skills(text)
    
    # Sort by count descending and get top N
    sorted_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    return dict(sorted_skills)


def match_skills(text: str, required_skills: List[str]) -> Dict[str, bool]:
    """
    Check which required skills are present in text
    
    Args:
        text: Input text to search
        required_skills: List of skills to look for
        
    Returns:
        Dictionary mapping skill to whether it was found
    """
    text_lower = text.lower()
    matches = {}
    
    for skill in required_skills:
        skill_lower = skill.lower()
        pattern = r'\b' + re.escape(skill_lower) + r'\b'
        matches[skill] = bool(re.search(pattern, text_lower))
    
    return matches


def calculate_skill_similarity(skills1: List[str], skills2: List[str]) -> float:
    """
    Calculate similarity between two skill sets using Jaccard similarity
    
    Args:
        skills1: First skill set
        skills2: Second skill set
        
    Returns:
        Similarity score between 0 and 1
    """
    set1 = set(s.lower() for s in skills1)
    set2 = set(s.lower() for s in skills2)
    
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    return intersection / union if union > 0 else 0.0


def get_all_skills() -> List[str]:
    """
    Get complete list of all tracked skills
    
    Returns:
        List of all skill names
    """
    return sorted([skill.title() for skill in TECH_SKILLS])


if __name__ == "__main__":
    # Test the skill extractor
    sample_text = """
    Senior Software Engineer with 5+ years of experience in Python and Java.
    Expertise in machine learning, deep learning, and cloud technologies (AWS, Azure).
    Strong background in Docker, Kubernetes, and CI/CD pipelines.
    Proficient in React, Node.js, and MongoDB for full-stack development.
    Experience with Spark, Kafka, and Airflow for data engineering.
    """
    
    print("=== Skill Extraction Test ===\n")
    
    skills = extract_skills(sample_text)
    print(f"Found {len(skills)} skills:")
    for skill, count in sorted(skills.items(), key=lambda x: x[1], reverse=True):
        print(f"  {skill}: {count}")
    
    print(f"\n=== Top 5 Skills ===")
    top_skills = extract_top_skills(sample_text, top_n=5)
    for skill, count in top_skills.items():
        print(f"  {skill}: {count}")
    
    print(f"\n=== Total unique skills in database: {len(TECH_SKILLS)} ===")
