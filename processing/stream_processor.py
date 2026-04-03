"""
stream_processor.py — Spark Structured Streaming: Kafka → Validate → Iceberg.

This is the core processing engine of the platform. It:
1. Reads raw telemetry events from Kafka topic 'telemetry.raw'
2. Parses the JSON and validates every field
3. Routes valid events to Iceberg table 'telemetry_db.clean_events'
4. Routes invalid events to Iceberg table 'telemetry_db.dead_letter' with rejection reasons

Think of it as a quality inspector on a factory conveyor belt:
  - Good products go to the warehouse (clean_events)
  - Defective products go to the reject bin with a note explaining the defect (dead_letter)

Usage:
    # Make sure Kafka and MinIO are running first
    docker compose up kafka minio -d

    # Run some data through Kafka
    python ingestion/fake_producer.py &

    # Start the stream processor
    python processing/stream_processor.py

Press Ctrl+C to stop.
"""

import sys
import os

# Add project root to Python path so we can import from storage/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

from storage.catalog import (
    get_spark_session, create_database, CATALOG_NAME, DATABASE_NAME
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry.raw"

# Where Spark saves its progress (which Kafka messages it's already processed)
CHECKPOINT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "checkpoints", "stream_processor"
)

# The 50 known device IDs — anything else is suspicious
VALID_DEVICE_IDS = {f"device_{i:03d}" for i in range(1, 51)}

# Allowed metric names (must match our Avro schema)
VALID_METRICS = {"cpu_usage", "memory_usage", "temperature", "disk_io", "network_latency"}

# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


def create_tables(spark):
    """Create the Iceberg tables if they don't already exist."""

    # Clean events table — validated, trustworthy data
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.clean_events (
            event_id STRING,
            timestamp TIMESTAMP,
            device_id STRING,
            metric_name STRING,
            value DOUBLE,
            unit STRING,
            location STRING,
            processed_at TIMESTAMP
        ) USING iceberg
    """)
    print("Table 'clean_events' ready.")

    # Dead letter table — rejected events with reasons
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.dead_letter (
            event_id STRING,
            original_data STRING,
            rejection_reason STRING,
            rejected_at TIMESTAMP
        ) USING iceberg
    """)
    print("Table 'dead_letter' ready.")


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------


def validate_event(row):
    """
    Check a single event against all validation rules.
    Returns (is_valid, reason) — reason is None if valid.
    """
    reasons = []

    # Rule 1: timestamp must not be null or in the future
    if row.timestamp is None:
        reasons.append("timestamp is null")
    elif row.timestamp.year >= 2099:
        reasons.append("timestamp is in the future")

    # Rule 2: device_id must be in our known list
    if row.device_id not in VALID_DEVICE_IDS:
        reasons.append(f"unknown device_id: {row.device_id}")

    # Rule 3: value must not be null or negative (for our metrics, negative is invalid)
    if row.value is None:
        reasons.append("value is null")
    elif row.value < 0:
        reasons.append(f"negative value: {row.value}")

    # Rule 4: metric_name must be in allowed list
    if row.metric_name not in VALID_METRICS:
        reasons.append(f"unknown metric: {row.metric_name}")

    if reasons:
        return False, "; ".join(reasons)
    return True, None


# ---------------------------------------------------------------------------
# Stream processing
# ---------------------------------------------------------------------------


def process_batch(batch_df, batch_id):
    """
    Called for each micro-batch of Kafka messages.

    This function:
    1. Parses the JSON from Kafka's value column
    2. Validates each event
    3. Writes valid events to clean_events
    4. Writes invalid events to dead_letter

    Spark calls this automatically — we don't call it ourselves.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession

    # --- Step 1: Parse the raw Kafka message bytes into structured columns ---
    # Kafka gives us: key (bytes), value (bytes), topic, partition, offset, timestamp
    # We only care about 'value' which contains our JSON event
    parsed = batch_df.select(
        F.from_json(
            F.col("value").cast("string"),
            StructType([
                StructField("event_id", StringType()),
                StructField("timestamp", StringType()),
                StructField("device_id", StringType()),
                StructField("metric_name", StringType()),
                StructField("value", DoubleType()),
                StructField("unit", StringType()),
                StructField("location", StringType()),
            ])
        ).alias("data"),
        F.col("value").cast("string").alias("raw_value")  # Keep raw JSON for dead letter
    ).select("data.*", "raw_value")

    # Convert timestamp string to actual Timestamp type
    parsed = parsed.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"))
    )

    # --- Step 2: Validate each event ---
    # We collect to the driver since micro-batches are small (a few seconds of data)
    rows = parsed.collect()

    valid_rows = []
    invalid_rows = []

    for row in rows:
        is_valid, reason = validate_event(row)
        if is_valid:
            valid_rows.append({
                "event_id": row.event_id,
                "timestamp": row.timestamp,
                "device_id": row.device_id,
                "metric_name": row.metric_name,
                "value": row.value,
                "unit": row.unit,
                "location": row.location,
            })
        else:
            invalid_rows.append({
                "event_id": row.event_id or "unknown",
                "original_data": row.raw_value,
                "rejection_reason": reason,
            })

    # --- Step 3: Write valid events to clean_events table ---
    if valid_rows:
        valid_df = spark.createDataFrame(valid_rows)
        valid_df = valid_df.withColumn("processed_at", F.current_timestamp())
        valid_df.writeTo(f"{DATABASE_NAME}.clean_events").append()
        print(f"  Batch {batch_id}: {len(valid_rows)} valid events → clean_events")

    # --- Step 4: Write invalid events to dead_letter table ---
    if invalid_rows:
        invalid_df = spark.createDataFrame(invalid_rows)
        invalid_df = invalid_df.withColumn("rejected_at", F.current_timestamp())
        invalid_df.writeTo(f"{DATABASE_NAME}.dead_letter").append()
        print(f"  Batch {batch_id}: {len(invalid_rows)} rejected events → dead_letter")

    if not valid_rows and not invalid_rows:
        print(f"  Batch {batch_id}: empty (no parseable events)")


def main():
    """Start the streaming pipeline."""

    print("=" * 60)
    print("STREAM PROCESSOR — Kafka → Validate → Iceberg")
    print("=" * 60)

    # --- Initialize Spark with Iceberg catalog ---
    spark = get_spark_session("StreamProcessor")
    create_database(spark)
    create_tables(spark)

    # --- Connect to Kafka and start reading ---
    print(f"\nReading from Kafka topic '{KAFKA_TOPIC}'...")
    print("Waiting for events... (Press Ctrl+C to stop)\n")

    # Read from Kafka as a streaming DataFrame
    # "earliest" means process all existing messages first, then wait for new ones
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Start processing — foreachBatch calls our process_batch function
    # for each micro-batch of messages that arrive from Kafka
    query = (
        kafka_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")  # Process every 10 seconds
        .start()
    )

    try:
        # Block until the user presses Ctrl+C
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping stream processor...")
        query.stop()
        spark.stop()
        print("Stream processor stopped.")


if __name__ == "__main__":
    main()
