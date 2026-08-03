"""
One genuine PySpark step: a full monthly rollup (avg/min/max/stddev MWh per
region + value_type + month) across the entire hourly_readings table,
read directly from Postgres via JDBC and written back as a new summary
table Tableau can also query.

Why Spark for this specific step, honestly: at ~1.3M rows this aggregation
is well within what a single pandas process could also handle — the point
of this script isn't "pandas couldn't do this," it's demonstrating real,
correctly-scoped hands-on use of Spark's DataFrame/JDBC API (schema
handling, groupBy/agg, writing results back out) on a real dataset,
locally, without overclaiming necessity that doesn't hold at this scale.
See README.md's Design Decisions for the full honest framing.

Deliberately small: one read, one groupBy/agg, one write. No streaming, no
UDFs, no cluster config — local[*] only, per the project's explicit scope.

Run with (from project root, main .venv active):
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 python src/spark_aggregation.py
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "spark_aggregation.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("spark_aggregation")

# Auto-fetched from Maven Central on first run (requires internet access,
# same as any pip install) — avoids manually downloading and managing a
# JDBC driver jar file for a one-script project.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def _jdbc_url_and_properties():
    """Build a JDBC URL from the same env vars warehouse.py uses, so this
    script points at whatever database the rest of the project does."""
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        # postgresql://user:pass@host/db?params -> jdbc:postgresql://host/db?params
        without_scheme = database_url.split("://", 1)[1]
        creds, hostpart = without_scheme.split("@", 1)
        user, password = creds.split(":", 1)
        jdbc_url = f"jdbc:postgresql://{hostpart}"
        return jdbc_url, {"user": user, "password": password, "driver": "org.postgresql.Driver"}

    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    return jdbc_url, {"user": user, "password": password, "driver": "org.postgresql.Driver"}


def run():
    jdbc_url, jdbc_props = _jdbc_url_and_properties()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("energy-sql-analytics-monthly-rollup")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Reading hourly_readings via JDBC from %s", jdbc_url)
        readings = spark.read.jdbc(url=jdbc_url, table="hourly_readings", properties=jdbc_props)

        n_rows = readings.count()
        logger.info("Loaded %s rows into Spark", n_rows)

        rollup = (
            readings
            .withColumn("year", F.year("reading_hour"))
            .withColumn("month", F.month("reading_hour"))
            .groupBy("region_code", "value_type", "year", "month")
            .agg(
                F.avg("value_mwh").alias("avg_mwh"),
                F.min("value_mwh").alias("min_mwh"),
                F.max("value_mwh").alias("max_mwh"),
                F.stddev("value_mwh").alias("stddev_mwh"),
                F.count("value_mwh").alias("n_hours"),
            )
            .orderBy("region_code", "value_type", "year", "month")
        )

        n_rollup_rows = rollup.count()
        logger.info("Rollup produced %s region/type/month rows", n_rollup_rows)

        logger.info("Writing rollup back to Postgres table monthly_region_rollup")
        rollup.write.jdbc(
            url=jdbc_url, table="monthly_region_rollup", mode="overwrite", properties=jdbc_props
        )

        logger.info("Done. %s hourly rows rolled up into %s monthly rows.", n_rows, n_rollup_rows)
        return {"input_rows": n_rows, "output_rows": n_rollup_rows}
    finally:
        spark.stop()


if __name__ == "__main__":
    result = run()
    print(f"Spark rollup complete: {result}")
