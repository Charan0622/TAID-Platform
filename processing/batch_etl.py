"""
batch_etl.py — Batch aggregation job: hourly stats per device per metric.

This Spark batch job reads from the clean_events Iceberg table and computes
hourly statistics (avg, min, max, stddev, count) grouped by device and metric.
Results are written to the hourly_aggregates Iceberg table.

Unlike the stream processor (which runs continuously), this is a one-shot job
meant to be run on-demand or scheduled by Airflow (Phase 4).

Think of it like closing the books at the end of each hour:
  "Device 001's CPU usage this hour: avg=45%, min=12%, max=87%, stddev=15%"

Usage:
    python processing/batch_etl.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from storage.catalog import get_spark_session, create_database, DATABASE_NAME


def create_aggregates_table(spark):
    """Create the hourly_aggregates Iceberg table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.hourly_aggregates (
            hour_window TIMESTAMP,
            device_id STRING,
            metric_name STRING,
            avg_value DOUBLE,
            min_value DOUBLE,
            max_value DOUBLE,
            stddev_value DOUBLE,
            event_count LONG,
            computed_at TIMESTAMP
        ) USING iceberg
    """)
    print("Table 'hourly_aggregates' ready.")


def run_aggregation(spark):
    """
    Read clean_events, compute hourly stats, write to hourly_aggregates.

    The key SQL concept here is GROUP BY + window:
      - window(timestamp, '1 hour') groups events into 1-hour buckets
      - We then compute aggregate functions (avg, min, max, stddev) within each group
    """

    # Read all clean events
    clean_events = spark.sql(f"SELECT * FROM {DATABASE_NAME}.clean_events")
    row_count = clean_events.count()

    if row_count == 0:
        print("No data in clean_events — nothing to aggregate.")
        return

    print(f"Processing {row_count} clean events...")

    # Compute hourly aggregations
    # window() creates 1-hour time buckets from the timestamp column
    # Then we group by (hour, device, metric) and compute stats
    aggregated = (
        clean_events
        .groupBy(
            F.window(F.col("timestamp"), "1 hour").alias("time_window"),
            F.col("device_id"),
            F.col("metric_name"),
        )
        .agg(
            F.round(F.avg("value"), 2).alias("avg_value"),
            F.round(F.min("value"), 2).alias("min_value"),
            F.round(F.max("value"), 2).alias("max_value"),
            F.round(F.stddev("value"), 2).alias("stddev_value"),
            F.count("*").alias("event_count"),
        )
        # Extract the start of the time window as a clean timestamp
        .withColumn("hour_window", F.col("time_window.start"))
        .drop("time_window")
        # Record when this aggregation was computed
        .withColumn("computed_at", F.current_timestamp())
    )

    agg_count = aggregated.count()
    print(f"Computed {agg_count} aggregation rows.")

    # Write to the hourly_aggregates table
    # We use overwritePartitions=False and just append, since we might re-run
    # for the same hour (Airflow retries). In production, you'd use MERGE for idempotency.
    aggregated.writeTo(f"{DATABASE_NAME}.hourly_aggregates").append()
    print(f"Written {agg_count} rows to hourly_aggregates.")


def main():
    print("=" * 60)
    print("BATCH ETL — Hourly Aggregations")
    print("=" * 60)

    spark = get_spark_session("BatchETL")
    create_database(spark)
    create_aggregates_table(spark)
    run_aggregation(spark)

    # Show a sample of results
    print("\nSample aggregations:")
    spark.sql(f"""
        SELECT hour_window, device_id, metric_name, avg_value, min_value, max_value, event_count
        FROM {DATABASE_NAME}.hourly_aggregates
        LIMIT 10
    """).show(truncate=False)

    spark.stop()
    print("Batch ETL complete.")


if __name__ == "__main__":
    main()
