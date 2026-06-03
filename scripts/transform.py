"""
transform.py
UHC Transparency in Coverage — Index File Transformer

Reads raw index JSON files from the landing zone, flattens the nested
structure and saves the result as Parquet to the processed zone.

Usage:
    python scripts/transform.py

Configuration (config/config.json):
    source:        source identifier (e.g. "uhc_tic")
    file_type:     file type to process (e.g. "index")
    repartition:   (optional) number of Parquet partitions — Spark decides if not set

Input:
    data/raw/{source}/{file_type}/{date}/*.json

Output:
    data/processed/{source}/{file_type}/
    └── partition_date=2026-06-01/
        └── part-00000.snappy.parquet

Output schema (17 columns):
    source_file, reporting_date, processed_at,
    reporting_entity_name, reporting_entity_type,
    last_updated_on, version,
    plan_name, plan_id, plan_id_type, plan_market_type,
    plan_sponsor_name, issuer_name,
    in_network_description, in_network_location,
    allowed_amount_description (*), allowed_amount_location (*)

    (*) nullable

Notes:
    - On Windows requires winutils.exe in C:/hadoop/bin
    - On Linux/Mac no additional setup needed
    - Files with empty in_network_files are kept as null rows (explode_outer)
    - reporting_date is duplicated as partition_date for Parquet partitioning
      so reporting_date remains inside the Parquet file for Snowflake COPY INTO
"""

import os
import json
import logging
import sys
import platform
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, explode_outer, col, input_file_name, lit, regexp_extract


def setup_logger() -> logging.Logger:
    """Setup logger with console and file handlers"""
    Path("logs").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/transform_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ]
    )
    return logging.getLogger(__name__)


def load_config(path: str = "config/config.json") -> dict:
    """Load configuration from JSON file"""
    with open(path) as f:
        return json.load(f)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a local SparkSession"""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def flatten_df(df: DataFrame, processed_at: str) -> DataFrame:
    """
    Flatten nested JSON structure to one row per plan per in_network_file.

    Input structure:
        1 row per file
        └── reporting_structure[]
            ├── reporting_plans[]
            └── in_network_files[]

    Output structure:
        1 row per reporting_structure x plan x in_network_file
    """

    # Add source_file — extract filename only from full path
    df = df.withColumn(
        "source_file",
        regexp_extract(input_file_name(), r"([^/\\]+)$", 1)
    )

    # Extract reporting_date from filename (e.g. "2026-06-01" from "2026-06-01_...")
    df = df.withColumn(
        "reporting_date",
        col("source_file").substr(1, 10)
    )

    # Add processed_at timestamp
    df = df.withColumn("processed_at", lit(processed_at))

    # Step 1: explode reporting_structure array
    df = df.withColumn("rs", explode(col("reporting_structure")))

    # Step 2: explode reporting_plans array inside each reporting_structure
    df = df.withColumn("plan", explode(col("rs.reporting_plans")))

    # Step 3: explode_outer in_network_files — keeps rows with empty array as null
    df = df.withColumn("in_net", explode_outer(col("rs.in_network_files")))

    # Step 4: select final flat columns
    # allowed_amount_file is optional → nullable columns
    df = df.select(
        col("source_file"),
        col("reporting_date"),
        col("processed_at"),
        col("reporting_entity_name"),
        col("reporting_entity_type"),
        col("last_updated_on"),
        col("version"),
        col("plan.plan_name"),
        col("plan.plan_id"),
        col("plan.plan_id_type"),
        col("plan.plan_market_type"),
        col("plan.plan_sponsor_name"),
        col("plan.issuer_name"),
        col("in_net.description").alias("in_network_description"),
        col("in_net.location").alias("in_network_location"),
        col("rs.allowed_amount_file.description").alias("allowed_amount_description"),
        col("rs.allowed_amount_file.location").alias("allowed_amount_location"),
    )

    return df


def save_parquet(df: DataFrame, output_dir: Path, logger: logging.Logger) -> None:
    """Save DataFrame to Parquet partitioned by partition_date"""
    output_path = str(output_dir).replace("\\", "/")
    logger.info(f"Saving to Parquet: {output_path}")

    # Duplicate reporting_date as partition_date so reporting_date stays
    # inside the Parquet file for Snowflake COPY INTO
    df = df.withColumn("partition_date", col("reporting_date"))

    df.write \
        .partitionBy("partition_date") \
        .mode("overwrite") \
        .parquet(output_path)

    logger.info("Parquet saved successfully")


def main():
    logger = setup_logger()
    logger.info("Starting transform job")

    # Set HADOOP_HOME for Windows — required for writing Parquet
    if platform.system() == "Windows":
        os.environ["HADOOP_HOME"] = "C:/hadoop"
        os.environ["PATH"] = os.environ["PATH"] + ";C:/hadoop/bin"

    # Load config
    config = load_config()
    RAW_DIR = Path("data/raw") / config["source"] / config["file_type"]
    PROCESSED_DIR = Path("data/processed") / config["source"] / config["file_type"]
    logger.info(f"Source: {config['source']} | File type: {config['file_type']}")
    logger.info(f"RAW_DIR: {RAW_DIR}")
    logger.info(f"PROCESSED_DIR: {PROCESSED_DIR}")

    # Capture start time — reused for processed_at and total processing time
    processed_at = datetime.now()
    logger.info(f"processed_at: {processed_at.isoformat()}")

    spark = create_spark_session(f"{config['source']}_{config['file_type']}_transform")
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session started")

    # Use Python glob — replace backslashes for Spark compatibility on Windows
    files = [str(f).replace("\\", "/") for f in RAW_DIR.glob("**/*.json")]
    logger.info(f"Files found: {len(files)}")

    # Read all files
    df = spark.read.option("multiline", "true").json(files)
    logger.info(f"Total rows before flatten: {df.count()}")

    # Flatten nested structure
    df_flat = flatten_df(df, processed_at.isoformat())
    logger.info(f"Total rows after flatten: {df_flat.count()}")

    # Data quality — null counts per column
    logger.info("Null counts per column:")
    for column in df_flat.columns:
        null_count = df_flat.filter(col(column).isNull()).count()
        logger.info(f"  {column}: {null_count}")

    # Alert if flatten returned 0 rows
    if df_flat.count() == 0:
        raise ValueError("Flatten returned 0 rows — check input files")

    # Apply repartition if specified in config — optional, Spark decides if not set
    repartition = config.get("repartition")
    if repartition is not None:
        df_flat = df_flat.repartition(repartition)
        logger.info(f"Repartitioned to {repartition} partition(s)")

    # Save to Parquet partitioned by partition_date
    save_parquet(df_flat, PROCESSED_DIR, logger)

    spark.stop()
    logger.info("Spark session stopped")
    logger.info(f"Total processing time: {datetime.now() - processed_at}")


if __name__ == "__main__":
    main()