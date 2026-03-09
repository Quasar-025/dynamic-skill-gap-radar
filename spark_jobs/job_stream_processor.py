import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, explode, split
from pyspark.sql.types import StructType, StructField, StringType
import json
import sys
from pathlib import Path

# Add current directory to path to import skill_extractor
sys.path.append(str(Path(__file__).parent))

try:
    from skill_extractor import TECH_SKILLS, extract_skills
    print("Successfully imported skill_extractor module")
    USING_SKILL_EXTRACTOR = True
except ImportError:
    print("Warning: Could not import skill_extractor, using fallback skills list")
    USING_SKILL_EXTRACTOR = False
    # Fallback to basic skills list
    TECH_SKILLS = [
        "python", "sql", "spark", "java", "scala", "aws", "azure", "gcp",
        "machine learning", "deep learning", "tensorflow", "pytorch", "docker",
        "kubernetes", "airflow", "kafka", "hadoop", "hive", "tableau", "power bi",
        "r programming", "pandas", "numpy", "scikit-learn", "git", "linux"
    ]

spark = SparkSession.builder \
    .appName("JobStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark streaming started...")

# Store current skill demand for API access
current_skill_demand = {}

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "job_postings") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert binary Kafka message → string
jobs = df.selectExpr("CAST(value AS STRING) as job_data")

def send_to_dashboard(batch_df, batch_id):
    """Process batch and send skill counts to dashboard"""
    global current_skill_demand
    
    print(f"\n=== Processing batch {batch_id} ===\")
    
    if batch_df.isEmpty():
        print("Empty batch, skipping...")
        return
    
    # Collect the batch data
    batch_data = batch_df.collect()
    
    # Count skills across all jobs in batch
    skill_counts = {}
    
    for row in batch_data:
        try:
            job_json = json.loads(row.job_data)
            job_text = ""
            
            # Concatenate relevant fields
            for field in ["Job Title", "Job Description", "Key Skills", "Qualifications"]:
                if field in job_json and job_json[field]:
                    job_text += str(job_json[field]).lower() + " "
            
            # Count skills mentioned in this job
            if USING_SKILL_EXTRACTOR:
                # Use enhanced skill extraction
                job_skills = extract_skills(job_text)
                for skill, count in job_skills.items():
                    skill_counts[skill] = skill_counts.get(skill, 0) + count
            else:
                # Fallback to simple pattern matching
                for skill in TECH_SKILLS:
                    if skill.lower() in job_text:
                        skill_counts[skill] = skill_counts.get(skill, 0) + 1
                    
        except Exception as e:
            print(f"Error processing job: {e}")
            continue
    
    # Sort by count and get top 10
    top_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    if top_skills:
        skills = [s[0].title() for s in top_skills]
        counts = [s[1] for s in top_skills]
        
        # Update global skill demand
        current_skill_demand = dict(zip(skills, counts))
        
        data = {
            "skills": skills,
            "counts": counts
        }
        
        print(f"Sending to dashboard: {data}")
        
        try:
            response = requests.post("http://localhost:8000/update", json=data, timeout=5)
            print(f"Dashboard response: {response.status_code}")
        except Exception as e:
            print(f"Error sending to dashboard: {e}")
    else:
        print("No skills found in batch")

# Write stream with foreachBatch
query = jobs.writeStream \
    .foreachBatch(send_to_dashboard) \
    .outputMode("append") \
    .start()

print("Stream processor running... Waiting for data from Kafka...")
query.awaitTermination()