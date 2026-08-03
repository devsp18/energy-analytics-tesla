-- Schema for the energy SQL analytics warehouse.
--
-- Design decisions:
--   * Two tables only (regions, hourly_readings) — this project's focus is
--     SQL analytics and query optimization on one large fact table, not a
--     multi-table quality-monitoring warehouse like energy-data-pipeline.
--   * Long format (one row per region + hour + value_type), matching the
--     EIA API's own shape and consistent with energy-data-pipeline's
--     design, rather than pivoting D/DF/NG/TI into separate columns. This
--     also naturally lands the row count in the target range: ~1.37M rows
--     in long format vs. ~342K if pivoted wide.
--   * PRIMARY KEY is the natural composite key (region_code, reading_hour,
--     value_type) — already guaranteed unique by the source data, and lets
--     loads be idempotent via ON CONFLICT without a surrogate-id lookup.
--   * Deliberately NO secondary indexes beyond what the primary key
--     creates implicitly. The whole point of this project's optimization
--     centerpiece is measuring a naive query against an unindexed table,
--     then adding exactly the index that query needs and proving the
--     difference — pre-indexing here would erase that comparison before
--     it starts. Any index added for the optimization step lives in
--     sql/optimization.sql, not here.

CREATE TABLE IF NOT EXISTS regions (
    region_code   TEXT PRIMARY KEY,        -- EIA respondent code, e.g. 'CAL', 'TEX'
    region_name   TEXT NOT NULL            -- human-readable name, e.g. 'California'
);

CREATE TABLE IF NOT EXISTS hourly_readings (
    region_code    TEXT NOT NULL REFERENCES regions(region_code),
    reading_hour   TIMESTAMPTZ NOT NULL,   -- UTC hour (EIA's "hourly" frequency, not local-hourly)
    -- D = demand, DF = day-ahead demand forecast, NG = net generation,
    -- TI = total interchange. Constrained rather than a lookup table since
    -- these four values are fixed by the EIA API itself.
    value_type     TEXT NOT NULL CHECK (value_type IN ('D', 'DF', 'NG', 'TI')),
    value_mwh      NUMERIC(14, 2) NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region_code, reading_hour, value_type)
);
