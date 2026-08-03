-- The one real optimization applied for this project (see README.md).
-- The naive query filters on value_type + a reading_hour range without
-- region_code, so it can't use the (region_code, reading_hour, value_type)
-- primary key. This index puts value_type first (equality filter, high
-- selectivity: 4 distinct values) and reading_hour second (range filter),
-- matching the actual WHERE clause so Postgres can serve it with an
-- index scan instead of a sequential scan.
CREATE INDEX IF NOT EXISTS idx_hourly_readings_type_hour
    ON hourly_readings (value_type, reading_hour);
