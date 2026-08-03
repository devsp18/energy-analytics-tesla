-- Optimized form of the same query, after adding
-- idx_hourly_readings_type_hour ON hourly_readings (value_type, reading_hour)
-- (see sql/add_optimization_index.sql). The SQL text is byte-identical to
-- naive_query.sql on purpose — the optimization is the new index letting
-- the planner pick an index scan for this filter, not a query rewrite.
-- tests/test_query_optimization.py proves both forms return identical rows.
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
