"""
Measurement harness for the optimization centerpiece.

Runs a query 5 times via EXPLAIN (ANALYZE, BUFFERS), discards the first run
(cold cache), averages the remaining 4, and saves both the timings and the
full plan text — so the optimization write-up in README.md is backed by a
reproducible measurement, not a single lucky/unlucky wall-clock number.

Usage:
    python src/benchmark_query.py --label naive --sql-file sql/naive_query.sql
    python src/benchmark_query.py --label optimized --sql-file sql/optimized_query.sql
"""

import argparse
import json
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:
    from src.warehouse import get_engine
except ImportError:
    from warehouse import get_engine


def run_explain_analyze(conn, query: str) -> tuple:
    """Returns (execution_time_ms, full_plan_text)."""
    result = conn.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}")
    plan_lines = [row[0] for row in result]
    plan_text = "\n".join(plan_lines)

    exec_time = None
    for line in plan_lines:
        match = re.search(r"Execution Time: ([\d.]+) ms", line)
        if match:
            exec_time = float(match.group(1))
    if exec_time is None:
        raise RuntimeError(f"Could not parse execution time from plan:\n{plan_text}")

    return exec_time, plan_text


def benchmark(query: str, n_runs: int = 5) -> dict:
    engine = get_engine()
    timings = []
    plans = []

    with engine.connect() as conn:
        for i in range(n_runs):
            exec_time, plan_text = run_explain_analyze(conn, query)
            timings.append(exec_time)
            plans.append(plan_text)
            print(f"  run {i + 1}/{n_runs}: {exec_time:.2f} ms")

    cold_run = timings[0]
    warm_runs = timings[1:]
    avg_warm = sum(warm_runs) / len(warm_runs)

    return {
        "all_timings_ms": timings,
        "cold_run_ms": cold_run,
        "warm_runs_ms": warm_runs,
        "avg_warm_ms": avg_warm,
        "plan_first_run": plans[0],
        "plan_last_run": plans[-1],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark a SQL query with EXPLAIN ANALYZE")
    parser.add_argument("--label", required=True, help="Label for this benchmark (e.g. naive, optimized)")
    parser.add_argument("--sql-file", required=True, help="Path to a .sql file containing exactly one query")
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    sql_text = Path(args.sql_file).read_text().strip().rstrip(";")

    print(f"Benchmarking '{args.label}' ({args.runs} runs, first discarded as cold cache)...")
    result = benchmark(sql_text, n_runs=args.runs)

    print()
    print(f"Cold run (discarded): {result['cold_run_ms']:.2f} ms")
    print(f"Warm runs: {[round(t, 2) for t in result['warm_runs_ms']]}")
    print(f"Average of warm runs: {result['avg_warm_ms']:.2f} ms")

    out_dir = PROJECT_ROOT / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"benchmark_{args.label}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nFull results (including EXPLAIN plans) saved to {out_path}")
