from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# Start Spark
spark = SparkSession.builder \
    .appName("SkillGapRadar_LoadJobs") \
    .getOrCreate()

print("Spark session started")

# Load CSV dataset
jobs_df = spark.read.csv(
    "data/raw/Uncleaned_DS_jobs.csv",
    header=True,
    inferSchema=True
)

print("Schema of dataset:")
jobs_df.printSchema()

print("Preview data:")
jobs_df.show(5)

# Basic cleaning
clean_jobs = jobs_df.dropDuplicates()

# Convert job descriptions to lowercase if column exists
if "Job Description" in clean_jobs.columns:
    clean_jobs = clean_jobs.withColumn(
        "Job Description",
        lower(col("Job Description"))
    )

print("Rows after cleaning:", clean_jobs.count())

# Save cleaned dataset as Parquet
clean_jobs.write.mode("overwrite").parquet(
    "data/processed/jobs_parquet"
)

print("Cleaned dataset saved to data/processed/jobs_parquet")