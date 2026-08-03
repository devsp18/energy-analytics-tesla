-- Naive form of the optimization centerpiece (see README.md "Query Optimization").
-- This is Query 1 from analysis_queries.sql, unmodified: it filters on
-- value_type + a reading_hour range but never touches region_code, the
-- leading column of the only index that exists (the primary key on
-- (region_code, reading_hour, value_type)). Postgres can't use that index
-- to serve this filter, so it falls back to a sequential scan over the
-- full hourly_readings table.
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
