"""
Correctness proof for the optimization centerpiece.

The naive and optimized queries (sql/naive_query.sql, sql/optimized_query.sql)
are byte-identical SQL text — the only thing that changed is the presence of
idx_hourly_readings_type_hour. So the real correctness question isn't "do two
different query texts agree," it's "does adding the index change the query's
results at all." This test drops the index, captures the result set, recreates
it, captures the result set again, and asserts they're identical row-for-row.
Faster but wrong would be worse than not doing this at all.
"""

from pathlib import Path

import pytest
from sqlalchemy import text

try:
    from src.warehouse import get_engine
except ImportError:
    from warehouse import get_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUERY_SQL = (PROJECT_ROOT / "sql" / "naive_query.sql").read_text().strip().rstrip(";")
INDEX_NAME = "idx_hourly_readings_type_hour"


def _run_query(conn):
    rows = conn.execute(text(QUERY_SQL)).fetchall()
    return [tuple(row) for row in rows]


def test_index_does_not_change_query_results():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {INDEX_NAME}"))
        conn.commit()
        results_without_index = _run_query(conn)

        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} "
                "ON hourly_readings (value_type, reading_hour)"
            )
        )
        conn.commit()
        results_with_index = _run_query(conn)

    assert results_without_index == results_with_index
    assert len(results_with_index) == 13  # one row per region


def test_optimized_query_file_is_identical_to_naive():
    """The optimization is the index, not a rewrite — guard against drift."""
    naive_sql = (PROJECT_ROOT / "sql" / "naive_query.sql").read_text()
    optimized_sql = (PROJECT_ROOT / "sql" / "optimized_query.sql").read_text()

    def _query_only(sql_text):
        return "\n".join(
            line for line in sql_text.splitlines() if not line.strip().startswith("--")
        ).strip()

    assert _query_only(naive_sql) == _query_only(optimized_sql)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
