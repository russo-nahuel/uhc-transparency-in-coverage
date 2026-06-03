# UHC Transparency in Coverage — Data Pipeline

End-to-end data pipeline that extracts, transforms and loads UHC Transparency in Coverage files into Snowflake for analytics.

---

## Architecture

```
UHC Public API
      │
      ▼
extract.py ──────────────► data/raw/{source}/{file_type}/{date}/
                                        │
                                        ▼
                           transform.py (PySpark)
                                        │
                                        ▼
                           data/processed/{source}/{file_type}/partition_date={date}/
                                        │
                                        ▼
                           load_snowflake.py
                                        │
                                        ▼
                           Snowflake: optum.uhc_tic.index_files
```

**Three file types supported:**

| File type        | Filter                        | Description                        |
|------------------|-------------------------------|------------------------------------|
| `index`          | `_index.json`                 | Table of contents (plain JSON)     |
| `in_network_rates` | `_in-network-rates.json.gz` | In-network rate files (compressed) |
| `allowed_amounts`  | `_allowed-amounts.json.gz`  | Allowed amount files (compressed)  |

---

## Prerequisites

- Python 3.12
- Java 17 (required for PySpark) — [Adoptium Temurin 17](https://adoptium.net/temurin/releases/?version=17)
- **Windows only:** `winutils.exe` in `C:/hadoop/bin` — required for PySpark to write Parquet
  - Download from [cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin)
- Snowflake trial account — [signup](https://signup.snowflake.com/)

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Configuration

All scripts read from `config/config.json`. Example for index files:

```json
{
    "source": "uhc_tic",
    "file_type": "index",
    "table_name": "index_files",
    "filter_endswith": "_index.json",
    "api_url": "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/",
    "max_files": 1000,
    "max_size_bytes": 1000000000,
    "repartition": 1
}
```

| Field            | Required | Description                                      |
|------------------|----------|--------------------------------------------------|
| `source`         | ✅        | Source identifier                                |
| `file_type`      | ✅        | File type to process                             |
| `table_name`     | ✅        | Snowflake table name                             |
| `filter_endswith`| ✅        | Filename filter                                  |
| `api_url`        | ✅        | UHC API endpoint                                 |
| `max_files`      | ✅        | Maximum number of files to download              |
| `max_size_bytes` | ❌        | Skip files larger than this size (optional)      |
| `repartition`    | ❌        | Number of Parquet partitions (optional)          |

Config examples for each file type: `config/examples/`

---

## Snowflake Setup

Set environment variables before running `load_snowflake.py`:

```bash
export SNOWFLAKE_ACCOUNT="zpijonz-wb07386"
export SNOWFLAKE_USER="NRUSSO"
export SNOWFLAKE_TOKEN="your_programmatic_access_token"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="OPTUM"
export SNOWFLAKE_SCHEMA="UHC_TIC"
```

Create the Snowflake table using `config/setup.sql` before the first load.

---

## Usage

Run the scripts in order:

```bash
# 1. Extract — download files from UHC API to landing zone
python scripts/extract.py

# 2. Transform — flatten JSON and save as Parquet
python scripts/transform.py

# 3. Load — load Parquet into Snowflake
python scripts/load_snowflake.py
```

---

## Data Structure

```
data/
├── raw/
│   └── uhc_tic/
│       └── index/
│           └── 2026-06-01/
│               ├── 2026-06-01_company_a_index.json
│               └── ...
└── processed/
    └── uhc_tic/
        └── index/
            └── partition_date=2026-06-01/
                └── part-00000.snappy.parquet
```

**Snowflake:**
```
optum (database)
└── uhc_tic (schema)
    └── index_files (table)
        └── 8868 rows — 17 columns
```

---

## Output Schema

| Column                     | Type      | Nullable | Description                              |
|----------------------------|-----------|----------|------------------------------------------|
| `source_file`              | VARCHAR   | ❌        | Source index filename                    |
| `reporting_date`           | DATE      | ❌        | Publication date (from filename)         |
| `processed_at`             | TIMESTAMP | ❌        | PySpark processing timestamp             |
| `reporting_entity_name`    | VARCHAR   | ❌        | UHC entity name                          |
| `reporting_entity_type`    | VARCHAR   | ❌        | Entity type (e.g. Third-Party Admin)     |
| `last_updated_on`          | DATE      | ❌        | Last update date (from JSON)             |
| `version`                  | VARCHAR   | ❌        | Schema version                           |
| `plan_name`                | VARCHAR   | ❌        | Health plan name                         |
| `plan_id`                  | VARCHAR   | ❌        | Employer Identification Number (EIN)     |
| `plan_id_type`             | VARCHAR   | ❌        | ID type (e.g. EIN)                       |
| `plan_market_type`         | VARCHAR   | ❌        | Market type (e.g. group)                 |
| `plan_sponsor_name`        | VARCHAR   | ✅        | Plan sponsor name                        |
| `issuer_name`              | VARCHAR   | ❌        | Issuer name                              |
| `in_network_description`   | VARCHAR   | ✅        | In-network file description              |
| `in_network_location`      | VARCHAR   | ✅        | In-network file URL                      |
| `allowed_amount_description` | VARCHAR | ✅        | Allowed amount file description          |
| `allowed_amount_location`  | VARCHAR   | ✅        | Allowed amount file URL                  |
| `loaded_at`                | TIMESTAMP | ✅        | Snowflake load timestamp (auto)          |

---

## Notes

- **Windows filesystem:** On case-insensitive filesystems (Windows), up to 4 files may be skipped due to filename case conflicts. This is a Windows-only issue — all files download correctly on Linux/Mac.
- **Snowflake authentication:** Uses programmatic access token instead of password — MFA is required on this account.
- **Load strategy:** Partition overwrite — deletes and reloads only the current `reporting_date`, preserving historical data for other periods.
