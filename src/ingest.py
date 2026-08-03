"""
Ingestion module for the EIA (U.S. Energy Information Administration) API v2.

Dataset: electricity/rto/region-data ("Hourly Demand, Demand Forecast, Net
Generation, and Interchange" — Form EIA-930, the Hourly Electric Grid
Monitor product). Route confirmed live against the API before writing this
(see session history / README) rather than assumed from documentation —
the same discipline used in the energy-data-pipeline project, where
guessing would have missed a real quirk.

Chosen over the daily-aggregated route used in energy-data-pipeline
specifically to get genuinely larger volume (hundreds of thousands to
low-millions of rows) for real SQL-optimization work: hourly granularity
across the same 13 EIA aggregate regions, but now across 4 value types
(D, DF, NG, TI) instead of 2, over a multi-year span.

Docs: https://www.eia.gov/opendata/documentation.php
API key: free registration at https://www.eia.gov/opendata/register.php
         -> set EIA_API_KEY in a local .env file (never hardcode it).
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# --- Paths -------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "logs"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "ingest.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("ingest")

# --- EIA API config ------------------------------------------------------
API_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

# Same 13 aggregated grid regions used in energy-data-pipeline (consistent
# with that project, and avoids the overlap/double-counting complexity of
# also including individual balancing authorities like PJM/MISO/CISO,
# several of which are components of these aggregate regions).
REGIONS = {
    "CAL": "California",
    "CAR": "Carolinas",
    "CENT": "Central",
    "FLA": "Florida",
    "MIDA": "Mid-Atlantic",
    "MIDW": "Midwest",
    "NE": "New England",
    "NW": "Northwest",
    "NY": "New York",
    "SE": "Southeast",
    "SW": "Southwest",
    "TEN": "Tennessee",
    "TEX": "Texas",
}

# D = demand, DF = day-ahead demand forecast, NG = net generation,
# TI = total interchange. All 4 (vs. just D/NG in energy-data-pipeline)
# specifically to support forecast-accuracy (D vs DF) and net-importer/
# -exporter (TI) analysis in the SQL analytics step, not just repeat the
# first project's scope at hourly grain.
VALUE_TYPES = ["D", "DF", "NG", "TI"]

# This route has no timezone-multiplication quirk (unlike daily-region-data
# in energy-data-pipeline, which reported each day 5x under different
# timezone conventions) — frequency is a single choice, not a facet.
# "hourly" = UTC hours, which keeps every region's hour boundary identical
# regardless of region (the same reasoning that pinned daily-region-data to
# a single timezone: comparing regions requires a shared clock).
FREQUENCY = "hourly"

# Real EIA-930 hourly data lags only a few hours behind real time (checked
# live: with "now" at 2026-08-02 ~07:00 UTC, the most recent available
# point was 2026-08-02T06) — nowhere near the ~1-2 day lag seen on the
# daily route. A conservative buffer avoids requesting an hour that hasn't
# posted yet on incremental/default pulls.
EIA_PUBLICATION_LAG_HOURS = 6

PAGE_SIZE = 5000  # EIA API v2 hard caps `length` at 5000 rows per request.
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2  # exponential backoff: 2, 4, 8, 16, 32 seconds
REQUEST_TIMEOUT_SECONDS = 30


class EIAIngestionError(Exception):
    """Raised when the EIA API cannot be reached or returns an unusable response."""


def _get_api_key() -> str:
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise EIAIngestionError(
            "EIA_API_KEY is not set. Register for a free key at "
            "https://www.eia.gov/opendata/register.php, then add it to a local "
            "'.env' file (see .env.example) — do not hardcode it in source."
        )
    return api_key


def _build_params(api_key: str, start: str, end: str, offset: int) -> dict:
    return {
        "api_key": api_key,
        "frequency": FREQUENCY,
        "data[0]": "value",
        "facets[respondent][]": list(REGIONS.keys()),
        "facets[type][]": VALUE_TYPES,
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": PAGE_SIZE,
    }


def _request_page(api_key: str, start: str, end: str, offset: int) -> dict:
    """Fetch one page of results, retrying transient failures with exponential backoff."""
    params = _build_params(api_key, start, end, offset)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.exceptions.Timeout:
            logger.warning(
                "Request timed out (offset=%s, attempt %s/%s)", offset, attempt, MAX_RETRIES
            )
            if attempt == MAX_RETRIES:
                raise EIAIngestionError(
                    f"EIA API timed out after {MAX_RETRIES} attempts at offset={offset}"
                )
            time.sleep(BACKOFF_BASE_SECONDS ** attempt)
            continue
        except requests.exceptions.ConnectionError as exc:
            logger.warning(
                "Connection error (offset=%s, attempt %s/%s): %s", offset, attempt, MAX_RETRIES, exc
            )
            if attempt == MAX_RETRIES:
                raise EIAIngestionError(
                    f"Could not connect to EIA API after {MAX_RETRIES} attempts: {exc}"
                )
            time.sleep(BACKOFF_BASE_SECONDS ** attempt)
            continue

        if response.status_code == 429 or response.status_code >= 500:
            logger.warning(
                "EIA API returned %s (offset=%s, attempt %s/%s)",
                response.status_code, offset, attempt, MAX_RETRIES,
            )
            if attempt == MAX_RETRIES:
                raise EIAIngestionError(
                    f"EIA API returned {response.status_code} after {MAX_RETRIES} retries: "
                    f"{response.text[:500]}"
                )
            time.sleep(BACKOFF_BASE_SECONDS ** attempt)
            continue

        if response.status_code >= 400:
            raise EIAIngestionError(
                f"EIA API request failed with {response.status_code}: {response.text[:500]}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise EIAIngestionError(f"EIA API returned non-JSON response: {exc}")

        if "response" not in payload or "data" not in payload["response"]:
            raise EIAIngestionError(
                f"Unexpected EIA API response shape (missing response.data): {payload}"
            )

        return payload

    raise EIAIngestionError("Exhausted retries without returning or raising")


def fetch_region_data(start: str, end: str) -> list:
    """
    Pull all hourly regional demand/forecast/generation/interchange records
    between start and end (inclusive, YYYY-MM-DDTHH), paginating through
    the EIA API until every row reported by the API's own `total` count has
    been collected.
    """
    api_key = _get_api_key()
    all_records = []
    offset = 0
    total = None

    while total is None or offset < total:
        logger.info("Fetching offset=%s (total so far known=%s)", offset, total)
        payload = _request_page(api_key, start, end, offset)
        resp = payload["response"]

        if total is None:
            total = int(resp.get("total", 0))
            logger.info("EIA reports %s total rows for this query", total)
            if total == 0:
                logger.warning("EIA API returned 0 rows for start=%s end=%s", start, end)
                break

        page_records = resp["data"]
        if not page_records:
            logger.warning("Empty page at offset=%s despite total=%s; stopping", offset, total)
            break

        all_records.extend(page_records)
        offset += len(page_records)

    logger.info("Fetched %s total records", len(all_records))
    return all_records


def save_raw(records: list, start: str, end: str) -> Path:
    """Persist the raw API response records to data/raw/ with a timestamped filename
    so every ingestion run is preserved (never overwritten) for auditability."""
    pulled_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"eia_hourly_region_data_{start}_to_{end}_{pulled_at}.json"
    out_path = RAW_DATA_DIR / filename

    payload = {
        "source": "EIA API v2 - electricity/rto/region-data",
        "pulled_at_utc": pulled_at,
        "start": start,
        "end": end,
        "record_count": len(records),
        "records": records,
    }

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    logger.info("Saved %s records to %s", len(records), out_path)
    return out_path


def run(start: str = None, end: str = None) -> Path:
    """Entry point used by both CLI and any scheduled/incremental pull."""
    if end is None:
        # Not literally "now" — see EIA_PUBLICATION_LAG_HOURS above.
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) \
            - timedelta(hours=EIA_PUBLICATION_LAG_HOURS)
        end = end_dt.strftime("%Y-%m-%dT%H")
    if start is None:
        # Default to a 3-year lookback: enough history for genuine
        # multi-year trend analysis while landing in the hundreds-of-
        # thousands-to-low-millions row range this project targets, without
        # pulling an unbounded amount of data on every run.
        start_dt = datetime.strptime(end, "%Y-%m-%dT%H") - timedelta(days=365 * 3)
        start = start_dt.strftime("%Y-%m-%dT%H")

    logger.info("Starting EIA hourly ingestion: start=%s end=%s", start, end)
    records = fetch_region_data(start, end)
    if not records:
        raise EIAIngestionError(
            f"No records fetched for start={start} end={end}; refusing to write empty raw file"
        )
    return save_raw(records, start, end)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest EIA hourly regional electricity data")
    parser.add_argument("--start", help="Start hour YYYY-MM-DDTHH (default: 3 years before end)")
    parser.add_argument("--end", help="End hour YYYY-MM-DDTHH (default: now minus publication lag)")
    args = parser.parse_args()

    out_path = run(start=args.start, end=args.end)
    print(f"Raw data written to: {out_path}")
