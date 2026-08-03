# Building the Tableau Public dashboard

This step needs the Tableau Public desktop app and your logged-in
`patelsatyam18` account — both interactive steps I can't perform. The three
CSVs in this folder are real exports straight from the loaded warehouse
(same numbers reported in the README), ready to connect directly.

## Files
- `monthly_region_rollup.csv` (1,924 rows) — the Spark-produced monthly
  rollup: avg/min/max/stddev MWh per region, value type, year, month. This
  is the one to build the main time-series view from.
- `net_importer_exporter.csv` (13 rows) — Query 3's result.
- `forecast_accuracy_by_region.csv` (13 rows) — Query 2's result.

## Suggested sheets (per the dataviz procedure: pick the form by the data's job first)

1. **Regional demand over time** — line chart, `monthly_region_rollup.csv`
   filtered to `value_type = D`, x = year+month, y = avg_mwh, color = region
   (fixed categorical order, one line per region, direct end-of-line labels
   for the top few so identity isn't color-only). This is a magnitude +
   change-over-time job → line chart is correct; resist adding a second
   y-axis for a second value type — use a filter or a second sheet instead.
2. **Forecast accuracy by region** — bar chart, `forecast_accuracy_by_region.csv`,
   sorted descending by `mean_abs_pct_error`. A single-measure ranking job →
   bar, not a table.
3. **Net importer/exporter** — diverging bar chart, `net_importer_exporter.csv`,
   `avg_hourly_interchange_mwh` on the axis, a diverging two-hue palette
   split at zero (importer vs. exporter), not a single sequential hue — this
   is a polarity job, not a plain magnitude job.
4. Combine as a dashboard with a region filter that cross-filters all three
   sheets.

## Publish
1. Open Tableau Public → Connect → Text File → select each CSV.
2. Build the sheets above, combine into one dashboard.
3. File → Save to Tableau Public As... → sign in as `patelsatyam18`.
4. Copy the published dashboard's public URL into the README's
   "Tableau Public dashboard" section, replacing the placeholder.
