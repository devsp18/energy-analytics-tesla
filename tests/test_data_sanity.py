"""Basic sanity checks on the loaded warehouse — not a data quality suite,
just a guard against a silently broken or partial load."""

from sqlalchemy import text

try:
    from src.warehouse import get_engine
except ImportError:
    from warehouse import get_engine

# The real EIA pull returned 1,364,369 rows (13 regions x 4 value types x
# ~3 years hourly). Bounded loosely rather than pinned exactly, since a
# fresh ingestion run picks up additional hours between "now" and the last
# run.
MIN_EXPECTED_ROWS = 1_300_000
MAX_EXPECTED_ROWS = 1_500_000
EXPECTED_VALUE_TYPES = {"D", "DF", "NG", "TI"}
EXPECTED_REGION_COUNT = 13


def test_row_count_in_expected_range():
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM hourly_readings")).scalar()
    assert MIN_EXPECTED_ROWS <= count <= MAX_EXPECTED_ROWS, (
        f"hourly_readings row count {count} outside expected range "
        f"[{MIN_EXPECTED_ROWS}, {MAX_EXPECTED_ROWS}]"
    )


def test_no_nulls_in_key_columns():
    engine = get_engine()
    with engine.connect() as conn:
        null_count = conn.execute(
            text(
                "SELECT COUNT(*) FROM hourly_readings "
                "WHERE region_code IS NULL OR reading_hour IS NULL "
                "OR value_type IS NULL OR value_mwh IS NULL"
            )
        ).scalar()
    assert null_count == 0


def test_value_types_match_expected_set():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT DISTINCT value_type FROM hourly_readings")).fetchall()
    actual_types = {row[0] for row in rows}
    assert actual_types == EXPECTED_VALUE_TYPES


def test_region_count_matches_expected():
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM regions")).scalar()
    assert count == EXPECTED_REGION_COUNT


def test_no_duplicate_natural_keys():
    engine = get_engine()
    with engine.connect() as conn:
        dupes = conn.execute(
            text(
                "SELECT COUNT(*) FROM ("
                "  SELECT region_code, reading_hour, value_type, COUNT(*) "
                "  FROM hourly_readings "
                "  GROUP BY region_code, reading_hour, value_type "
                "  HAVING COUNT(*) > 1"
                ") dupes"
            )
        ).scalar()
    assert dupes == 0
