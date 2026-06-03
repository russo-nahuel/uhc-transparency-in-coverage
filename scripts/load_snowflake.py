"""
load_snowflake.py
UHC Transparency in Coverage — Snowflake Loader

Loads processed Parquet files into Snowflake using internal stage and COPY INTO.
Uses partition overwrite strategy — deletes and reloads only the current partition,
preventing duplicates on reload while preserving historical data for other periods.

Usage:
    python scripts/load_snowflake.py

Environment variables required:
    SNOWFLAKE_ACCOUNT:    account identifier (e.g. "zpijonz-wb07386")
    SNOWFLAKE_USER:       username (e.g. "NRUSSO")
    SNOWFLAKE_TOKEN:      programmatic access token
    SNOWFLAKE_WAREHOUSE:  warehouse name (e.g. "COMPUTE_WH")
    SNOWFLAKE_DATABASE:   database name (e.g. "OPTUM")
    SNOWFLAKE_SCHEMA:     schema name (e.g. "UHC_TIC")

Configuration (config/config.json):
    source:       source identifier (e.g. "uhc_tic")
    file_type:    file type to load (e.g. "index")
    table_name:   Snowflake table name (e.g. "index_files")

Input:
    data/processed/{source}/{file_type}/
    └── partition_date=2026-06-01/
        └── part-00000.snappy.parquet

Load strategy:
    1. Create internal stage
    2. Upload Parquet to stage
    3. DELETE existing rows for reporting_date (partition overwrite)
    4. COPY INTO table from stage
    5. Verify row count matches Parquet
    6. Rollback on failure — table never left empty

Notes:
    - Credentials loaded from environment variables
    - Logs saved to logs/load_snowflake_{timestamp}.log
"""

import os
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
import snowflake.connector


def setup_logger() -> logging.Logger:
    """Setup logger with console and file handlers"""
    Path("logs").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/load_snowflake_{timestamp}.log"

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


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Connect to Snowflake using environment variables"""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def get_reporting_date(processed_dir: Path) -> str:
    """Extract reporting_date from partition folder name"""
    partition_dirs = list(processed_dir.glob("partition_date=*"))
    if not partition_dirs:
        raise ValueError(f"No partition_date folder found in {processed_dir}")
    return partition_dirs[0].name.split("=")[1]


def count_parquet_rows(processed_dir: Path, logger: logging.Logger) -> int:
    """Count total rows in Parquet files"""
    files = list(processed_dir.glob("**/*.parquet"))
    total = sum(pq.read_metadata(str(f)).num_rows for f in files)
    logger.info(f"Total rows in Parquet: {total}")
    return total


def create_stage(cur, logger: logging.Logger) -> None:
    """Create internal stage for Parquet files"""
    logger.info("Creating stage...")
    cur.execute("CREATE STAGE IF NOT EXISTS parquet_stage")
    logger.info("Stage ready")


def upload_parquet(cur, processed_dir: Path, logger: logging.Logger) -> None:
    """Upload Parquet files to Snowflake stage"""
    files = list(processed_dir.glob("**/*.parquet"))
    logger.info(f"Uploading {len(files)} Parquet file(s) to stage...")

    for file in files:
        file_path = str(file).replace("\\", "/")
        cur.execute(f"PUT file://{file_path} @parquet_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        logger.info(f"Uploaded: {file.name}")


def delete_partition(cur, table_name: str, reporting_date: str, logger: logging.Logger) -> None:
    """Delete existing data for reporting_date — partition overwrite strategy"""
    logger.info(f"Deleting existing data for reporting_date: {reporting_date}")
    cur.execute(f"DELETE FROM {table_name} WHERE reporting_date = '{reporting_date}'")
    deleted = cur.rowcount
    logger.info(f"Deleted {deleted} rows for reporting_date: {reporting_date}")


def copy_into_table(cur, table_name: str, logger: logging.Logger) -> None:
    """Load data from stage into table"""
    logger.info("Loading data into table...")
    cur.execute(f"""
        COPY INTO {table_name} (
            source_file,
            reporting_date,
            processed_at,
            reporting_entity_name,
            reporting_entity_type,
            last_updated_on,
            version,
            plan_name,
            plan_id,
            plan_id_type,
            plan_market_type,
            plan_sponsor_name,
            issuer_name,
            in_network_description,
            in_network_location,
            allowed_amount_description,
            allowed_amount_location
        )
        FROM (
            SELECT
                $1:source_file::VARCHAR,
                $1:reporting_date::DATE,
                $1:processed_at::TIMESTAMP,
                $1:reporting_entity_name::VARCHAR,
                $1:reporting_entity_type::VARCHAR,
                $1:last_updated_on::DATE,
                $1:version::VARCHAR,
                $1:plan_name::VARCHAR,
                $1:plan_id::VARCHAR,
                $1:plan_id_type::VARCHAR,
                $1:plan_market_type::VARCHAR,
                $1:plan_sponsor_name::VARCHAR,
                $1:issuer_name::VARCHAR,
                $1:in_network_description::VARCHAR,
                $1:in_network_location::VARCHAR,
                $1:allowed_amount_description::VARCHAR,
                $1:allowed_amount_location::VARCHAR
            FROM @parquet_stage
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PURGE = TRUE
    """)
    logger.info("Data loaded successfully")


def verify_row_count(cur, table_name: str, reporting_date: str, parquet_rows: int, logger: logging.Logger) -> None:
    """Verify rows loaded match Parquet row count"""
    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE reporting_date = '{reporting_date}'")
    loaded_rows = cur.fetchone()[0]
    logger.info(f"Rows expected: {parquet_rows} | Rows loaded: {loaded_rows}")

    if loaded_rows != parquet_rows:
        raise ValueError(f"Row count mismatch: expected {parquet_rows}, got {loaded_rows}")

    logger.info("Row count verification passed")


def main():
    logger = setup_logger()
    logger.info("Starting Snowflake load job")

    # Load config
    config = load_config()
    processed_dir = Path("data/processed") / config["source"] / config["file_type"]
    table_name = config["table_name"]
    logger.info(f"Source: {config['source']} | File type: {config['file_type']} | Table: {table_name}")
    logger.info(f"Processed dir: {processed_dir}")

    # Extract reporting_date from partition folder
    reporting_date = get_reporting_date(processed_dir)
    logger.info(f"Reporting date: {reporting_date}")

    # Count Parquet rows — alert if 0
    parquet_rows = count_parquet_rows(processed_dir, logger)
    if parquet_rows == 0:
        raise ValueError("No rows found in Parquet files — check processed directory")

    conn = get_connection()
    logger.info("Connected to Snowflake")

    cur = conn.cursor()

    create_stage(cur, logger)
    upload_parquet(cur, processed_dir, logger)

    # Delete + load with rollback on failure
    try:
        delete_partition(cur, table_name, reporting_date, logger)
        copy_into_table(cur, table_name, logger)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, rolling back: {e}")
        raise

    verify_row_count(cur, table_name, reporting_date, parquet_rows, logger)

    # Total rows in table
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    logger.info(f"Total rows in table: {count}")

    conn.close()
    logger.info("Connection closed")


if __name__ == "__main__":
    main()