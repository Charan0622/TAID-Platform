"""
catalog.py — Iceberg catalog configuration for the telemetry platform.

This module provides a reusable function to create a SparkSession that's
pre-configured to work with Iceberg tables stored in MinIO. Every Spark job
in this project imports from here so the configuration lives in one place.

What is a catalog?
    Think of it like a phone book for database tables. When you query
    "telemetry_db.clean_events", the catalog knows where the actual data
    files (Parquet) live on disk (in MinIO). Without it, Spark wouldn't
    know where to find or write table data.

What is Iceberg?
    Iceberg makes regular files (Parquet) behave like a proper database table.
    It adds transactions (writes either fully succeed or fully fail),
    schema enforcement (data must match the table structure), and
    versioning (every write creates a "snapshot" — you can query any
    past version, like time travel in a database).
"""

import os
import subprocess

from pyspark.sql import SparkSession

# Ensure JAVA_HOME is set — PySpark needs it to start the JVM.
# This handles cases where the shell hasn't sourced ~/.zshrc (e.g., running from an IDE).
if not os.environ.get("JAVA_HOME"):
    try:
        java_home = subprocess.check_output(
            ["/usr/libexec/java_home", "-v", "17"], text=True
        ).strip()
        os.environ["JAVA_HOME"] = java_home
    except Exception:
        pass  # Fall through — PySpark will give a clear error if Java isn't found

# ---------------------------------------------------------------------------
# Configuration — all Spark jobs share these settings
# ---------------------------------------------------------------------------

# MinIO connection details (matches docker-compose.yml)
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Where Iceberg stores table data in MinIO
WAREHOUSE_PATH = "s3a://telemetry-warehouse/"

# Iceberg catalog name — used in SQL like: SELECT * FROM telemetry_catalog.db.table
CATALOG_NAME = "telemetry_catalog"

# Database name inside the catalog
DATABASE_NAME = "telemetry_db"


def get_spark_session(app_name: str = "TelemetryPlatform") -> SparkSession:
    """
    Create and return a SparkSession configured for Iceberg + MinIO.

    Args:
        app_name: Name shown in Spark UI and logs (helps identify which job is running)

    Returns:
        A ready-to-use SparkSession with Iceberg catalog configured.
    """

    # The Iceberg Spark runtime JAR — this adds Iceberg support to Spark.
    # Must match our Spark version (3.5) and Scala version (2.12).
    # Spark downloads this automatically from Maven Central on first run.
    iceberg_jar = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"

    # AWS SDK bundle — needed because MinIO speaks the S3 protocol.
    # Spark uses the AWS SDK to talk to MinIO as if it were Amazon S3.
    aws_jar = "org.apache.iceberg:iceberg-aws-bundle:1.7.1"

    # Hadoop AWS connector — provides the S3AFileSystem class that Hadoop/Spark
    # uses to read/write files on S3-compatible storage (MinIO).
    hadoop_aws_jar = "org.apache.hadoop:hadoop-aws:3.3.4"

    # Kafka connector — lets Spark read/write from Kafka topics as streaming sources.
    # Without this, Spark throws "Failed to find data source: kafka".
    kafka_jar = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

    session = (
        SparkSession.builder
        .appName(app_name)

        # --- Spark resource limits (protect our 16GB laptop) ---
        .config("spark.driver.memory", "2g")         # Max 2GB for the Spark driver
        .config("spark.sql.shuffle.partitions", "8")  # Default 200 is overkill locally

        # --- Download and load the Iceberg + AWS JARs ---
        .config("spark.jars.packages", f"{iceberg_jar},{aws_jar},{hadoop_aws_jar},{kafka_jar}")

        # --- Tell Spark to use Iceberg's extensions for CREATE TABLE, etc. ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # --- Configure our Iceberg catalog ---
        # Register a catalog named "telemetry_catalog"
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        # Use Hadoop catalog type — stores metadata as files in MinIO
        # (no external metastore database needed)
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.type",
            "hadoop"
        )
        # Where the catalog stores table data and metadata
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            WAREHOUSE_PATH
        )

        # --- Configure S3/MinIO connection ---
        # Tell Hadoop's S3A filesystem to use our MinIO credentials
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        # Required for MinIO — use path-style URLs (bucket.endpoint → endpoint/bucket)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Use the simple file system (no checksums) — faster for local dev
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Disable SSL since MinIO runs on plain HTTP locally
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # --- Set Iceberg as the default catalog so we don't have to prefix every query ---
        .config("spark.sql.defaultCatalog", CATALOG_NAME)

        .getOrCreate()
    )

    # Set log level to WARN to reduce noisy INFO messages during development
    session.sparkContext.setLogLevel("WARN")

    return session


def create_database(spark: SparkSession) -> None:
    """
    Create the telemetry database if it doesn't exist.

    This is idempotent — safe to call multiple times.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    print(f"Database '{DATABASE_NAME}' ready.")
