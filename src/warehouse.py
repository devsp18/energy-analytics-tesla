"""
Loads raw EIA hourly region-data JSON into the PostgreSQL warehouse defined
in sql/schema.sql.

This project has no separate cleaning module by design — unlike
energy-data-pipeline (which needed dedup/outlier-flagging/missing-value
policy as first-class deliverables), this project's real content is the
SQL analytics, optimization, and Spark work downstream. The minimal,
unavoidable type coercion a load always needs lives here instead of a
dedicated clean.py, to keep the project's weight where it actually belongs.

Uses SQLAlchemy Core's `INSERT ... ON CONFLICT DO UPDATE` (batch upsert,
not per-row) on the natural key (region_code, reading_hour, value_type),
so a rerun over the same or overlapping data updates rows in place instead
of creating duplicates.
"""

import glob
import json
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "warehouse.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("warehouse")

# Same EIA non-numeric sentinels seen in energy-data-pipeline (withheld/
# not-yet-available data points reported as text instead of a number).
EIA_MISSING_SENTINELS = {"", "NM", "W", ".", "NA", "null", None}

REGION_NAMES = {
    "CAL": "California", "CAR": "Carolinas", "CENT": "Central", "FLA": "Florida",
    "MIDA": "Mid-Atlantic", "MIDW": "Midwest", "NE": "New England", "NW": "Northwest",
    "NY": "New York", "SE": "Southeast", "SW": "Southwest", "TEN": "Tennessee",
    "TEX": "Texas",
}


class WarehouseError(Exception):
    """Raised when the warehouse cannot be reached or a load fails."""


def get_engine():
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(url, pool_pre_ping=True)

    required = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise WarehouseError(
            f"Missing required env vars: {missing}. Copy .env.example to .env and fill them in."
        )

    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, pool_pre_ping=True)


def find_latest_raw_file() -> Path:
    files = sorted(glob.glob(str(RAW_DATA_DIR / "eia_hourly_region_data_*.json")))
    if not files:
        raise FileNotFoundError(f"No raw ingestion files found in {RAW_DATA_DIR}. Run src/ingest.py first.")
    return Path(files[-1])


def load_raw_to_dataframe(path: Path) -> pd.DataFrame:
    logger.info("Loading raw file: %s", path)
    with open(path) as f:
        payload = json.load(f)

    records = payload["records"]
    df = pd.DataFrame.from_records(records)
    logger.info("Loaded %s raw rows from %s", len(df), path)

    df = df.rename(columns={
        "period": "reading_hour",
        "respondent": "region_code",
        "respondent-name": "region_name",
        "type": "value_type",
        "value": "value_mwh",
    })

    # EIA's hourly period format is "YYYY-MM-DDTHH" in UTC (frequency=hourly,
    # not local-hourly) — parsed explicitly so a format drift surfaces here
    # loudly rather than silently producing NaT downstream.
    df["reading_hour"] = pd.to_datetime(df["reading_hour"], format="%Y-%m-%dT%H", utc=True)

    df["region_code"] = df["region_code"].astype(str).str.strip().str.upper()
    df["value_type"] = df["value_type"].astype(str).str.strip().str.upper()
    df["region_name"] = df["region_code"].map(REGION_NAMES)

    df["value_mwh"] = df["value_mwh"].where(~df["value_mwh"].isin(EIA_MISSING_SENTINELS))
    df["value_mwh"] = pd.to_numeric(df["value_mwh"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["reading_hour", "region_code", "value_type", "value_mwh"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %s rows with missing key fields or missing value_mwh", dropped)

    before = len(df)
    df = df.drop_duplicates(subset=["region_code", "reading_hour", "value_type"], keep="last")
    if before - len(df):
        logger.warning("Deduplicated %s rows on the natural key", before - len(df))

    return df[["region_code", "region_name", "reading_hour", "value_type", "value_mwh"]]


def load_regions(engine, df: pd.DataFrame) -> int:
    metadata = MetaData()
    regions_table = Table("regions", metadata, autoload_with=engine)

    regions_df = df[["region_code", "region_name"]].drop_duplicates()
    records = regions_df.to_dict(orient="records")

    stmt = pg_insert(regions_table).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["region_code"],
        set_={"region_name": stmt.excluded.region_name},
    )
    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info("Upserted %s region rows", len(records))
    return len(records)


def load_hourly_readings(engine, df: pd.DataFrame, chunk_size: int = 5000) -> int:
    metadata = MetaData()
    readings_table = Table("hourly_readings", metadata, autoload_with=engine)

    load_df = df[["region_code", "reading_hour", "value_type", "value_mwh"]]
    records = load_df.to_dict(orient="records")

    total_loaded = 0
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = pg_insert(readings_table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["region_code", "reading_hour", "value_type"],
                set_={"value_mwh": stmt.excluded.value_mwh},
            )
            conn.execute(stmt)
            total_loaded += len(chunk)
            if total_loaded % 100000 == 0 or total_loaded == len(records):
                logger.info("Upserted %s / %s hourly_readings rows", total_loaded, len(records))

    return total_loaded


def run(raw_path: Path = None) -> dict:
    if raw_path is None:
        raw_path = find_latest_raw_file()
    engine = get_engine()
    df = load_raw_to_dataframe(raw_path)
    if df.empty:
        raise WarehouseError(f"{raw_path} produced zero usable rows; refusing to load")

    n_regions = load_regions(engine, df)
    n_readings = load_hourly_readings(engine, df)
    return {"regions_upserted": n_regions, "readings_upserted": n_readings}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load raw EIA hourly data into the Postgres warehouse")
    parser.add_argument("--raw-file", help="Path to a specific raw JSON file (default: latest)")
    args = parser.parse_args()

    result = run(Path(args.raw_file) if args.raw_file else None)
    print(f"Load complete: {result}")
