"""
Hive-backed persistence for scraped job market data.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, explode, lower, trim, row_number, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)


DB_NAME = "skill_gap"
POSTINGS_TABLE = f"{DB_NAME}.job_postings"
DEMAND_TABLE = f"{DB_NAME}.skill_demand"


def build_hive_spark(app_name: str = "SkillGapHiveStore") -> SparkSession:
    warehouse_dir = Path("data/hive/warehouse").resolve()
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    return spark


def postings_schema() -> StructType:
    return StructType(
        [
            StructField("job_uid", StringType(), False),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("company", StringType(), True),
            StructField("location", StringType(), True),
            StructField("role", StringType(), True),
            StructField("description", StringType(), True),
            StructField("url", StringType(), True),
            StructField("scraped_at", StringType(), True),
            StructField("skills", ArrayType(StringType()), True),
        ]
    )


def upsert_postings(spark: SparkSession, rows: List[Dict]) -> int:
    if not rows:
        return 0

    incoming = spark.createDataFrame(rows, schema=postings_schema())

    if spark.catalog.tableExists(POSTINGS_TABLE):
        existing = spark.table(POSTINGS_TABLE)
        merged = existing.unionByName(incoming, allowMissingColumns=True)
    else:
        merged = incoming

    # Keep the newest scrape for each job UID so recommendations use fresh market data.
    window = Window.partitionBy("job_uid").orderBy(to_timestamp(col("scraped_at")).desc_nulls_last())
    combined = (
        merged
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    staging_table = f"{DB_NAME}.job_postings_staging"
    combined.write.mode("overwrite").saveAsTable(staging_table)

    if spark.catalog.tableExists(POSTINGS_TABLE):
        spark.sql(f"DROP TABLE {POSTINGS_TABLE}")
    spark.sql(f"ALTER TABLE {staging_table} RENAME TO {POSTINGS_TABLE}")

    final_postings = spark.table(POSTINGS_TABLE)
    _materialize_parquet_snapshots(final_postings)
    rebuild_demand_table(spark)

    return final_postings.count()


def rebuild_demand_table(spark: SparkSession) -> None:
    if not spark.catalog.tableExists(POSTINGS_TABLE):
        return

    postings = spark.table(POSTINGS_TABLE)

    demand = (
        postings
        .withColumn("role_norm", lower(trim(col("role"))))
        .withColumn("company_norm", lower(trim(col("company"))))
        .withColumn("location_norm", lower(trim(col("location"))))
        .withColumn("skill", explode(col("skills")))
        .groupBy("role_norm", "company_norm", "location_norm", "skill")
        .count()
        .withColumnRenamed("count", "demand_count")
    )

    demand.write.mode("overwrite").saveAsTable(DEMAND_TABLE)
    _materialize_demand_snapshot(demand)


def _materialize_parquet_snapshots(postings_df) -> None:
    output = Path("data/processed/market")
    output.mkdir(parents=True, exist_ok=True)
    postings_df.write.mode("overwrite").parquet(str(output / "job_postings.parquet"))


def _materialize_demand_snapshot(demand_df) -> None:
    output = Path("data/processed/market")
    output.mkdir(parents=True, exist_ok=True)
    demand_df.write.mode("overwrite").parquet(str(output / "skill_demand.parquet"))


def fetch_market_demand(
    spark: SparkSession,
    role: str | None = None,
    company: str | None = None,
    top_n: int = 20,
) -> Dict[str, int]:
    if not spark.catalog.tableExists(DEMAND_TABLE):
        return {}

    df = spark.table(DEMAND_TABLE)
    if role:
        df = df.filter(lower(col("role_norm")) == role.lower().strip())
    if company:
        df = df.filter(lower(col("company_norm")) == company.lower().strip())

    top = (
        df.groupBy("skill")
        .sum("demand_count")
        .withColumnRenamed("sum(demand_count)", "demand_count")
        .orderBy(col("demand_count").desc())
        .limit(top_n)
        .collect()
    )

    return {row["skill"]: int(row["demand_count"]) for row in top}


def fetch_role_company_options(spark: SparkSession, limit: int = 200) -> Dict[str, List[str]]:
    if not spark.catalog.tableExists(POSTINGS_TABLE):
        return {"roles": [], "companies": []}

    postings = spark.table(POSTINGS_TABLE)

    roles = [
        row["role"]
        for row in postings.select("role").where(col("role").isNotNull()).dropDuplicates().limit(limit).collect()
        if row["role"]
    ]
    companies = [
        row["company"]
        for row in postings.select("company").where(col("company").isNotNull()).dropDuplicates().limit(limit).collect()
        if row["company"]
    ]

    return {
        "roles": sorted(roles),
        "companies": sorted(companies),
    }
