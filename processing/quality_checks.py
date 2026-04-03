"""
quality_checks.py — Data quality validation for Iceberg tables.

Runs a series of SQL-based checks against our clean_events table to verify
the data is healthy. Each check returns PASS or FAIL with details.

These checks are like a doctor's checkup for your data:
  - Is data arriving? (row count check)
  - Is anything missing? (null rate check)
  - Are values sensible? (range check)
  - Are there duplicates? (uniqueness check)

Used standalone or triggered by Airflow as a quality gate in the pipeline.

Usage:
    python processing/quality_checks.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.catalog import get_spark_session, create_database, DATABASE_NAME


def check_row_count(spark, min_threshold=1):
    """
    CHECK 1: Are there any events at all?
    A pipeline that produces zero rows is broken even if it doesn't throw errors.
    """
    result = spark.sql(f"""
        SELECT COUNT(*) as total_rows FROM {DATABASE_NAME}.clean_events
    """).collect()[0]

    total = result.total_rows
    passed = total >= min_threshold
    return {
        "check": "row_count",
        "passed": passed,
        "detail": f"Total rows: {total} (threshold: >= {min_threshold})",
    }


def check_null_rates(spark, max_null_rate=0.01):
    """
    CHECK 2: Are any columns mostly null?
    If more than 1% of values in a column are null, something upstream is broken.
    """
    result = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_id,
            SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamp,
            SUM(CASE WHEN device_id IS NULL THEN 1 ELSE 0 END) as null_device_id,
            SUM(CASE WHEN metric_name IS NULL THEN 1 ELSE 0 END) as null_metric_name,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_value,
            SUM(CASE WHEN unit IS NULL THEN 1 ELSE 0 END) as null_unit,
            SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) as null_location
        FROM {DATABASE_NAME}.clean_events
    """).collect()[0]

    total = result.total
    if total == 0:
        return {"check": "null_rates", "passed": False, "detail": "No data to check"}

    # Check each column's null rate
    columns = ["event_id", "timestamp", "device_id", "metric_name", "value", "unit", "location"]
    failures = []

    for col in columns:
        null_count = getattr(result, f"null_{col}")
        null_rate = null_count / total
        if null_rate > max_null_rate:
            failures.append(f"{col}: {null_rate:.2%} null (>{max_null_rate:.0%})")

    passed = len(failures) == 0
    detail = "All columns within threshold" if passed else "; ".join(failures)
    return {"check": "null_rates", "passed": passed, "detail": detail}


def check_value_ranges(spark):
    """
    CHECK 3: Are metric values within expected ranges?
    After validation, we shouldn't see negative values or values over 1000.
    """
    result = spark.sql(f"""
        SELECT
            MIN(value) as min_val,
            MAX(value) as max_val,
            SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) as negative_count,
            SUM(CASE WHEN value > 1000 THEN 1 ELSE 0 END) as extreme_count
        FROM {DATABASE_NAME}.clean_events
    """).collect()[0]

    negative = result.negative_count
    extreme = result.extreme_count
    passed = negative == 0 and extreme == 0

    detail = (
        f"Range: [{result.min_val:.2f}, {result.max_val:.2f}], "
        f"negatives: {negative}, extremes(>1000): {extreme}"
    )
    return {"check": "value_ranges", "passed": passed, "detail": detail}


def check_duplicates(spark):
    """
    CHECK 4: Are there duplicate event IDs?
    Each event_id should be unique (UUID). Duplicates mean our exactly-once
    processing broke, or the producer sent the same event twice.
    """
    result = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT event_id) as unique_ids
        FROM {DATABASE_NAME}.clean_events
    """).collect()[0]

    total = result.total
    unique = result.unique_ids
    duplicates = total - unique
    passed = duplicates == 0

    detail = f"Total: {total}, Unique: {unique}, Duplicates: {duplicates}"
    return {"check": "duplicates", "passed": passed, "detail": detail}


def run_all_checks(spark):
    """Run all quality checks and return results."""
    checks = [
        check_row_count(spark),
        check_null_rates(spark),
        check_value_ranges(spark),
        check_duplicates(spark),
    ]
    return checks


def main():
    print("=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)

    spark = get_spark_session("QualityChecks")
    create_database(spark)
    results = run_all_checks(spark)

    # Display results
    all_passed = True
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        icon = "+" if result["passed"] else "X"
        print(f"  [{icon}] {result['check']:20s} {status:4s} — {result['detail']}")
        if not result["passed"]:
            all_passed = False

    print()
    if all_passed:
        print("All quality checks PASSED.")
    else:
        print("Some quality checks FAILED. Investigate before proceeding.")

    spark.stop()
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
