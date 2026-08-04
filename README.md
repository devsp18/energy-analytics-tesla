# Energy SQL Analytics: Query Optimization & Spark on Real Grid Data

A second, focused portfolio project: pull a genuinely large real dataset from
the U.S. Energy Information Administration, write advanced SQL analytics
against it, then **prove** - with measured before/after numbers and a
correctness test, not just a claim - that a specific optimization made a
specific query faster without changing its answer. A small, honestly-scoped
PySpark step rounds it out.

**Honesty note:** this project runs entirely on real, public EIA data. It has
no connection to Tesla's internal systems, data, or infrastructure - it was
built as a portfolio project to demonstrate SQL and data engineering skills
relevant to a Tesla Energy internship application (Data Engineer, Energy -
Req 271331), using the closest real-world analog of grid-scale energy data
that's actually public.

This is a separate, from-scratch project from `energy-data-pipeline` - no
code, database, or environment is shared between them.

## Why hourly over daily

`energy-data-pipeline` (the first project) used EIA's
`electricity/rto/daily-region-data` route. This project instead uses
**`electricity/rto/region-data`** - the hourly Electric Grid Monitor product
(Form EIA-930) - specifically because it's an order of magnitude larger: 13
regions × 4 value types × 3 years of hourly readings is **1,364,369 rows**,
versus the low tens of thousands the daily route produces over the same
span. The optimization centerpiece below needs a real table where an
unindexed scan is actually slow enough to matter; the daily route's row
count wouldn't have gotten there.

The exact route was confirmed live against the real API before any pulling
started, the same discipline used in the first project - nothing here was
guessed from documentation alone.

One real difference from the daily route, discovered while building this:
this hourly route does **not** re-aggregate each reading under multiple
timezone conventions (the daily route's `timezone` facet quirk). Frequency
here is a single `hourly` vs `local-hourly` choice, so ingestion didn't need
the timezone-pinning workaround the first project required.

## Architecture

```
                 ┌───────────────────────────┐
                 │   EIA API v2               │
                 │ electricity/rto/           │
                 │ region-data (hourly)       │
                 └──────────┬─────────────────┘
                            │  requests (retries, backoff, pagination)
                            ▼
                 ┌───────────────────────────┐
                 │  src/ingest.py             │  raw JSON → data/raw/
                 └──────────┬─────────────────┘
                            ▼
                 ┌───────────────────────────┐
                 │  src/warehouse.py          │  parse, coerce, dedup,
                 │                            │  idempotent upsert
                 └──────────┬─────────────────┘
                            ▼
        ┌───────────────────────────────────────────┐
        │              PostgreSQL                     │
        │  regions | hourly_readings (1,364,369 rows) │
        │  monthly_region_rollup (written by Spark)    │
        └───────┬───────────────────┬──────────────────┘
                │                   │
                ▼                   ▼
   ┌─────────────────────┐   ┌───────────────────────────┐
   │ sql/analysis_queries │   │  src/spark_aggregation.py  │
   │  window fn / CTE /   │   │  JDBC read → groupBy/agg    │
   │  multi-table join    │   │  → JDBC write                │
   └─────────────────────┘   └───────────────────────────┘
                │
                ▼
   ┌─────────────────────────────────────────┐
   │  src/benchmark_query.py                   │
   │  EXPLAIN (ANALYZE, BUFFERS) x5, discard    │
   │  cold run, average 4 warm runs             │
   │  naive (seq scan) vs optimized (idx scan)  │
   └─────────────────────────────────────────┘
                │
                ▼
        Tableau Public dashboard
```

## Project structure

```
energy-sql-analytics/
├── src/
│   ├── ingest.py            # EIA API → data/raw/*.json
│   ├── warehouse.py         # raw JSON → Postgres (idempotent upsert)
│   ├── benchmark_query.py   # EXPLAIN ANALYZE measurement harness
│   └── spark_aggregation.py # the one PySpark step
├── sql/
│   ├── schema.sql                  # warehouse DDL (2 tables, deliberately no secondary indexes)
│   ├── analysis_queries.sql        # the 3 advanced SQL queries + insight comments
│   ├── naive_query.sql             # optimization centerpiece: before
│   ├── optimized_query.sql         # optimization centerpiece: after (same SQL, new index)
│   └── add_optimization_index.sql  # the one index that changes the plan
├── tests/
│   ├── test_query_optimization.py  # proves the index doesn't change results
│   └── test_data_sanity.py         # row count range, no nulls, no dupes
├── data/{raw,processed}/     # gitignored - real pulled data + benchmark JSON land here
├── logs/                     # gitignored
├── requirements.txt          # main app deps
├── requirements-spark.txt    # PySpark, checked for Python/Java compatibility first
└── README.md                 # this file
```

## Running it locally

### 1. EIA API key
Register (free, instant) at https://www.eia.gov/opendata/register.php. Reuses
the same key pattern as `energy-data-pipeline`, but this project has its own
`.env`.

### 2. Environment variables
```bash
cp .env.example .env
# edit .env: EIA_API_KEY, Postgres credentials
```

### 3. PostgreSQL
Reuses the same native Homebrew PostgreSQL install as the first project - a
new database and role, no reinstall:
```bash
createdb energy_sql_analytics
psql energy_sql_analytics -f sql/schema.sql
```

### 4. Main venv
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python src/ingest.py       # pulls real EIA hourly data → data/raw/
python src/warehouse.py --raw-file data/raw/<the file ingest.py wrote>
```

### 5. SQL analytics
```bash
psql energy_sql_analytics -f sql/analysis_queries.sql
```

### 6. Optimization centerpiece
```bash
python src/benchmark_query.py --label naive --sql-file sql/naive_query.sql
psql energy_sql_analytics -f sql/add_optimization_index.sql
python src/benchmark_query.py --label optimized --sql-file sql/optimized_query.sql
```

### 7. PySpark step
```bash
pip install -r requirements-spark.txt
JAVA_HOME=/opt/homebrew/opt/openjdk@17 python src/spark_aggregation.py
```

### 8. Tests
```bash
pytest tests/ -v
```

## Advanced SQL analytics

Each query in `sql/analysis_queries.sql` was written to answer one specific
question this dataset can actually answer - run for real against all
1,364,369 loaded rows.

**Query 1 - window function: regional generation ranking.** Ranks all 13
regions by average hourly net generation (`value_type = 'NG'`) for January
2025 using `RANK() OVER (ORDER BY AVG(value_mwh) DESC)`. Real result:
**Mid-Atlantic (PJM) generates the most** at 114,224.8 MWh/hr average - about
1.26x the next region (Midwest, 90,817.2 MWh/hr) - while **New England
generates the least** at 11,862.8 MWh/hr, about a tenth of Mid-Atlantic's
output. This is also the query used as the optimization centerpiece below.

**Query 2 - CTE + self-join: day-ahead forecast accuracy.** A CTE
(`forecast_vs_actual`) self-joins `hourly_readings` on
`region_code + reading_hour` to line up each hour's actual demand (`D`)
against its day-ahead forecast (`DF`), then aggregates mean absolute error
and MAPE per region. Real result: **California has the least accurate
day-ahead demand forecasts** (7.29% MAPE, 2,266.1 MWh average absolute
error), while **Southeast is the most accurate** (1.93% MAPE). That's a
genuine ~3.8x gap in forecast quality between two real grid regions.

**Query 3 - multi-table join: net importers vs. exporters.** Joins
`hourly_readings` (filtered to `value_type = 'TI'`, total interchange)
against the `regions` dimension table for readable names, then classifies
each region by the sign of its average hourly interchange (EIA convention:
positive = net export, negative = net import). Real result: **California is
the largest net importer** of power (-4,712.2 MWh/hr average) and **the
Northwest is the largest net exporter** (+3,222.1 MWh/hr average) - both
consistent with what's publicly known about California's grid relying on
imports and the Pacific Northwest's hydro surplus.

## Query optimization - the centerpiece

**The query.** Query 1 above filters on `value_type` and a `reading_hour`
range but never touches `region_code` - the leading column of the only index
that exists, the primary key on `(region_code, reading_hour, value_type)`.
That index can't serve this filter, so Postgres has to fall back to scanning
the whole table.

**Methodology.** Each version was run 5 times via
`EXPLAIN (ANALYZE, BUFFERS)`; the first run (cold cache) was discarded and
the remaining 4 were averaged. `src/benchmark_query.py` automates this and
saves the full timings and plan text for both runs, so the numbers below are
reproducible, not hand-picked.

**Before (naive, no secondary index):**

| run | time (ms) |
|---|---|
| 1 (cold, discarded) | 48.59 |
| 2 | 29.27 |
| 3 | 22.37 |
| 4 | 21.32 |
| 5 | 21.14 |
| **average of warm runs** | **23.53 ms** |

Plan: `Parallel Seq Scan on hourly_readings`, filtering `value_type = 'NG'`
and the date range, **removing 451,566 rows per worker** (3 loops) just to
find the ~3,224 rows per worker that matched. Total buffer reads: **11,080**.

**The optimization:** one index, matching the actual filter columns:

```sql
CREATE INDEX idx_hourly_readings_type_hour
    ON hourly_readings (value_type, reading_hour);
```

`value_type` leads because it's an equality filter over only 4 distinct
values (high selectivity for a range condition to narrow further);
`reading_hour` follows because it's the range condition. This is the one
optimization applied - no query rewrite, since the query itself was already
a reasonable way to ask the question; the only real problem was the missing
index.

**After (optimized, same SQL text, new index):**

| run | time (ms) |
|---|---|
| 1 (cold, discarded) | 10.70 |
| 2 | 5.64 |
| 3 | 5.00 |
| 4 | 4.87 |
| 5 | 3.65 |
| **average of warm runs** | **4.79 ms** |

Plan: `Bitmap Index Scan on idx_hourly_readings_type_hour` →
`Bitmap Heap Scan`, both conditions applied directly as an index condition.
Total buffer reads: **329** - a 33.7x drop in actual I/O, not just a faster
clock.

**Result: ~4.9x faster (23.53ms → 4.79ms average warm), and the proof is the
plan change (sequential scan over the full table → index scan hitting only
the matching rows), which timing alone couldn't fake if, say, the second run
just happened to be warmer.**

**Correctness check.** Faster but wrong would be worse than not doing this
at all. `tests/test_query_optimization.py::test_index_does_not_change_query_results`
drops the index, runs the query, recreates the index, runs it again, and
asserts the two result sets are identical row-for-row - proving the index
changes the plan, not the answer. A second test guards that
`naive_query.sql` and `optimized_query.sql` stay byte-identical (aside from
comments), since the whole point is that the optimization is the index, not
a rewrite.

## The Spark step

`src/spark_aggregation.py` does one thing: reads all of `hourly_readings`
from Postgres via JDBC into a Spark DataFrame, computes a monthly rollup
(avg/min/max/stddev of `value_mwh`, grouped by region + value type + year +
month), and writes the 1,924-row result back to Postgres as
`monthly_region_rollup`. Run for real:

```
Loaded 1364369 rows into Spark
Rollup produced 1924 region/type/month rows
Spark rollup complete: {'input_rows': 1364369, 'output_rows': 1924}
```

**Why Spark, honestly, for this specific step:** at ~1.3M rows this
aggregation is well within what a single pandas process could also handle -
the point of this script isn't "pandas couldn't do this." It's demonstrating
real, correctly-scoped, hands-on use of Spark's DataFrame and JDBC APIs
(schema inference over a JDBC source, a real `groupBy`/`agg`, writing
results back out) on a genuine dataset, running locally
(`local[*]`, no cluster config, no streaming, no UDFs). Overclaiming that
Spark was *necessary* here would be dishonest at this row count; the goal
was genuine exposure, not manufactured necessity.

**Before installing anything:** PySpark 4.2.0's PyPI metadata was checked
live - it lists Python 3.14 as a supported classifier and ships as a
pure-Python sdist with no C-extension wheels, so unlike Airflow in the first
project, no separate virtual environment was needed. Spark's own docs were
then checked for its Java requirement (17/21/25); this machine only had Java
11, so `openjdk@17` was installed via Homebrew as a keg-only formula
(`JAVA_HOME` set per-invocation) without disturbing the existing Java 11
default.

## Tests

```bash
pytest tests/ -v
```

- `test_query_optimization.py` - the naive-vs-optimized correctness proof
  described above, plus a guard that the two SQL files stay in sync.
- `test_data_sanity.py` - row count in the expected range
  (1.3M–1.5M, loose because a fresh ingestion run picks up new hours since
  the last one), no nulls in key columns, the exact expected set of 4 value
  types, 13 regions, and no duplicate natural keys.

All 7 tests pass against the real loaded warehouse.

## Design decisions

**Why no `clean.py` this time.** The first project's real content was the
cleaning pipeline (outlier detection, missing-value handling, a
`data_quality_log`). This project's real content is downstream - the SQL
analytics, the optimization proof, and Spark - so `warehouse.py` does the
minimal parsing and coercion needed to load real EIA data safely (sentinel
handling, dedup on the natural key) without duplicating the first project's
work.

**Why no secondary indexes in the initial schema.** `sql/schema.sql`
deliberately ships with only the primary key. Adding an index up front would
have hidden the actual optimization story - there needed to be a genuine
unindexed baseline to measure against, not a schema already tuned for the
query it's supposed to justify.

**Why a composite natural key again.**
`(region_code, reading_hour, value_type)` is unique in the source data and
makes loads idempotent via `ON CONFLICT ... DO UPDATE`, the same reasoning
as the first project's `sql/schema.sql`.

**Why the EIA publication lag constant is smaller here (6 hours vs. 2
days).** The daily route in the first project had a 1-2 day lag; this hourly
route's most recent available hour was only a few hours behind real time
when checked live. `EIA_PUBLICATION_LAG_HOURS = 6` reflects what was
actually observed, not a copied assumption from the other project.

**Why buffer reads, not just milliseconds, are the real optimization
proof.** Wall-clock time is sensitive to whatever else the machine is doing
and to page-cache state - a second "cold" run can look fast just from
caching, without the plan actually changing. The 11,080 → 329 buffer-read
drop and the seq-scan → index-scan plan change are structural, not
timing-dependent - which is why both are reported side by side with the
milliseconds, not instead of them.

## Tableau Public dashboard

[To be added - see repo for current status.]
