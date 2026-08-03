-- Advanced SQL analytics against the real EIA hourly warehouse.
-- Each query is written to answer one specific, real question this
-- dataset actually supports — not to demonstrate syntax for its own sake.
-- Real results and the insight each one produced are documented in
-- README.md's "Advanced SQL Analytics" section.

-- ============================================================================
-- Query 1: Window function — regional net generation ranking for a month
--
-- Real question: which regions generated the most/least power in a given
-- month, ranked? This is also the query used as the optimization
-- centerpiece (see sql/optimization.sql) — as written here, it filters on
-- value_type + a reading_hour range without region_code, which the
-- (region_code, reading_hour, value_type) primary key can't serve
-- efficiently, forcing a sequential scan over the full table.
-- ============================================================================
SELECT
    region_code,
    ROUND(AVG(value_mwh), 1) AS avg_hourly_net_generation_mwh,
    RANK() OVER (ORDER BY AVG(value_mwh) DESC) AS generation_rank
FROM hourly_readings
WHERE value_type = 'NG'
  AND reading_hour >= '2025-01-01'
  AND reading_hour < '2025-02-01'
GROUP BY region_code
ORDER BY generation_rank;

-- ============================================================================
-- Query 2: CTE + self-join — day-ahead demand forecast accuracy by region
--
-- Real question: which regions have the least accurate day-ahead demand
-- forecasts? Joins the 'D' (actual demand) and 'DF' (day-ahead forecast)
-- rows for the same region+hour via a self-join inside a CTE, then
-- aggregates mean absolute error and MAPE by region.
-- ============================================================================
WITH forecast_vs_actual AS (
    SELECT
        d.region_code,
        d.reading_hour,
        d.value_mwh AS actual_demand_mwh,
        df.value_mwh AS forecast_demand_mwh
    FROM hourly_readings d
    JOIN hourly_readings df
      ON df.region_code = d.region_code
     AND df.reading_hour = d.reading_hour
     AND df.value_type = 'DF'
    WHERE d.value_type = 'D'
)
SELECT
    region_code,
    ROUND(AVG(ABS(actual_demand_mwh - forecast_demand_mwh)), 1) AS mean_abs_error_mwh,
    ROUND(
        AVG(ABS(actual_demand_mwh - forecast_demand_mwh)) / NULLIF(AVG(actual_demand_mwh), 0) * 100,
        2
    ) AS mean_abs_pct_error
FROM forecast_vs_actual
GROUP BY region_code
ORDER BY mean_abs_pct_error DESC;

-- ============================================================================
-- Query 3: Multi-table join — net importer vs. net exporter regions
--
-- Real question: which regions are, on average, net importers vs. net
-- exporters of power? Joins the fact table against the regions dimension
-- table for human-readable names (a genuine multi-table join, distinct
-- from query 2's same-table self-join).
-- EIA convention for total interchange (TI): positive = net export to
-- neighboring balancing authorities, negative = net import.
-- ============================================================================
SELECT
    r.region_code,
    r.region_name,
    ROUND(AVG(h.value_mwh), 1) AS avg_hourly_interchange_mwh,
    CASE WHEN AVG(h.value_mwh) < 0 THEN 'Net Importer' ELSE 'Net Exporter' END AS interchange_role
FROM hourly_readings h
JOIN regions r ON r.region_code = h.region_code
WHERE h.value_type = 'TI'
GROUP BY r.region_code, r.region_name
ORDER BY avg_hourly_interchange_mwh;
