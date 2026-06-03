"""
extract.py
UHC Transparency in Coverage — File Extractor

Connects to the UHC Transparency in Coverage public API and downloads
files to the local landing zone (data/raw/).

Supports three file types configured via config/config.json:
    - index:            table of contents files (_index.json)
    - in_network_rates: in-network rate files (_in-network-rates.json.gz)
    - allowed_amounts:  allowed amount files (_allowed-amounts.json.gz)

Usage:
    python scripts/extract.py

Configuration (config/config.json):
    source:           source identifier (e.g. "uhc_tic")
    file_type:        file type to download (e.g. "index")
    filter_endswith:  filename filter (e.g. "_index.json")
    api_url:          UHC Transparency in Coverage API endpoint
    max_files:        maximum number of files to download
    max_size_bytes:   (optional) skip files larger than this size

Configuration examples for each file type: config/examples/

Output structure:
    data/raw/{source}/{file_type}/{date}/
    └── data/raw/uhc_tic/index/2026-06-01/
        ├── 2026-06-01_company_a_index.json
        └── 2026-06-01_company_b_index.json

Notes:
    - Resumable: skips already downloaded files
    - Handles individual download errors without stopping
    - On Windows, files may be skipped due to case-insensitive
      filesystem conflicts — not an issue on Linux/Mac
"""

import requests
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://transparency-in-coverage.uhc.com/",
}

def setup_logger() -> logging.Logger:
    """Setup logger with console and file handlers"""
    Path("logs").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/extract_{timestamp}.log"

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

def load_config(path: str = "config/config.json"):
    """Load configuration from JSON file"""
    with open(path) as f:
        return json.load(f)

def sanitize_filename(name: str, max_length: int = 130) -> str:
    """Replace spaces with underscores and truncate if necessary"""
    name = name.replace(" ", "_")
    if len(name) > max_length:
        ext = "".join(Path(name).suffixes)
        name = name[:max_length - len(ext)] + ext
    return name

def get_blobs(api_url: str, logger: logging.Logger) -> list:
    """Call the API and return the raw list of blobs"""
    logger.info("Connecting to API...")
    response = requests.get(api_url, headers=HEADERS)
    logger.info(f"Status code: {response.status_code}")
    response.raise_for_status()
    return response.json().get("blobs", [])

def filter_blobs(blobs: list, config: dict, logger: logging.Logger) -> list:
    """Filter blobs by file_type, max_size_bytes and max_files"""

    # Filter by file type
    type_filtered = []
    for blob in blobs:
        if blob["name"].endswith(config["filter_endswith"]):
            type_filtered.append(blob)
    logger.info(f"Blobs after filter_endswith filter: {len(type_filtered)}")

    # Filter by max_size_bytes (optional)
    max_size_bytes = config.get("max_size_bytes")
    if max_size_bytes is not None:
        size_filtered = []
        for blob in type_filtered:
            if blob["size"] <= max_size_bytes:
                size_filtered.append(blob)
        logger.info(f"Blobs after max_size_bytes filter: {len(size_filtered)}")
    else:
        size_filtered = type_filtered

    # Apply max_files limit
    filtered = size_filtered[:config["max_files"]]
    logger.info(f"Blobs after max_files limit: {len(filtered)}")

    return filtered

def download_files(blobs: list, output_dir: Path, logger: logging.Logger) -> None:
    """Download files to raw output_dir, skipping already downloaded files"""

    downloaded = 0
    skipped = 0
    failed = 0

    for blob in tqdm(blobs, desc="Downloading", unit="file", dynamic_ncols=True, file=sys.stdout):

        # Extract date partition from filename (e.g. "2026-06-01")
        reporting_date = blob["name"][:10]
        file_dir = output_dir / reporting_date
        file_dir.mkdir(parents=True, exist_ok=True)

        file_path = file_dir / sanitize_filename(blob["name"])

        if file_path.exists():
            skipped += 1
            continue

        try:
            response = requests.get(blob["downloadUrl"], headers=HEADERS)
            response.raise_for_status()
            file_path.write_bytes(response.content)
            downloaded += 1

        except Exception as e:
            failed += 1
            tqdm.write(f"Failed: {blob['name']} — {e}")
            logger.error(f"Failed: {blob['name']} — {e}")

    # Summary
    logger.info(f"Summary — Downloaded: {downloaded} | Skipped: {skipped} | Failed: {failed}")

def main():
    logger = setup_logger()
    config = load_config()
    blobs = get_blobs(config["api_url"], logger)
    logger.info(f"Total blobs: {len(blobs)}")

    filtered = filter_blobs(blobs, config, logger)
    logger.info(f"Ready to download: {len(filtered)} files")

    output_dir = Path("data") / "raw" / config["source"] / config["file_type"]
    logger.info(f"Output directory: {output_dir}")
    download_files(filtered, output_dir, logger)

if __name__ == "__main__":
    main()