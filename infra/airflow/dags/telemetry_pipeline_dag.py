"""
telemetry_pipeline_dag.py — Main data pipeline DAG for the telemetry platform.

This DAG runs every 15 minutes and orchestrates:
1. Check Kafka consumer lag (is data flowing?)
2. Trigger the stream processor (Kafka → validate → Iceberg)
3. Run data quality checks (is the data healthy?)
4. Quality gate decision (pass → continue, fail → alert)
5. Run batch aggregation (hourly stats)
6. Update metadata catalog
7. Notify success

DAG = Directed Acyclic Graph — a workflow of tasks with dependencies.
Think of it as a recipe: "check ingredients → cook → taste-test → serve."

Task flow:
    check_kafka_lag → run_stream_processor → run_quality_checks → quality_gate
        ├── (pass) → run_batch_aggregation → update_metadata → notify_success
        └── (fail) → alert_quality_failure
"""

from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# DAG default arguments — apply to every task unless overridden
# ---------------------------------------------------------------------------
default_args = {
    "owner": "telemetry-team",       # Who "owns" this pipeline (for filtering in UI)
    "depends_on_past": False,         # Don't wait for yesterday's run to succeed
    "email_on_failure": False,        # No email alerts (we're local)
    "email_on_retry": False,
    "retries": 2,                     # Retry failed tasks up to 2 times
    "retry_delay": timedelta(minutes=1),  # Wait 1 minute between retries
}


# ---------------------------------------------------------------------------
# Task functions — the actual logic each task runs
# ---------------------------------------------------------------------------

def check_kafka_lag(**context):
    """
    Task 1: Verify Kafka is running and the topic has data.

    In production, you'd check consumer group lag (how far behind the consumer
    is from the latest message). For local dev, we verify connectivity.
    """
    from kafka import KafkaConsumer

    try:
        consumer = KafkaConsumer(
            bootstrap_servers="host.docker.internal:9092",
            consumer_timeout_ms=5000,
        )
        topics = consumer.topics()
        consumer.close()

        if "telemetry.raw" in topics:
            print(f"Kafka healthy. Available topics: {topics}")
            return "healthy"
        else:
            print(f"Topic 'telemetry.raw' not found. Topics: {topics}")
            return "no_topic"
    except Exception as e:
        print(f"Kafka connection failed: {e}")
        raise


def run_stream_processor(**context):
    """
    Task 2: Trigger Spark Structured Streaming processing.

    In a full deployment, this would SSH to the Spark host or submit a job
    via spark-submit. For local dev, we log the intent — the actual stream
    processor runs natively on the host (Phase 3).
    """
    print("=" * 50)
    print("STREAM PROCESSOR TASK")
    print("=" * 50)
    print("In production: would trigger spark-submit for stream_processor.py")
    print("Locally: stream processor runs natively on host machine")
    print("Status: SIMULATED SUCCESS")
    print("=" * 50)
    return "stream_complete"


def run_quality_checks(**context):
    """
    Task 3: Execute data quality validations.

    Returns a JSON summary of check results for the quality gate.
    For local dev, we simulate the checks passing (real checks run natively).
    """
    # Simulate quality check results
    # In production, this would call the quality_checks.py script
    results = {
        "row_count": {"passed": True, "detail": "Row count above threshold"},
        "null_rates": {"passed": True, "detail": "All columns within threshold"},
        "value_ranges": {"passed": True, "detail": "All values in expected range"},
        "duplicates": {"passed": True, "detail": "No duplicate event IDs"},
    }

    all_passed = all(r["passed"] for r in results.values())

    # Push results to XCom so downstream tasks can read them
    # XCom = "cross-communication" — Airflow's way of passing data between tasks
    context["ti"].xcom_push(key="quality_results", value=results)
    context["ti"].xcom_push(key="all_passed", value=all_passed)

    print(f"Quality check results: {json.dumps(results, indent=2)}")
    print(f"All passed: {all_passed}")
    return results


def quality_gate(**context):
    """
    Task 4: Branch based on quality check results.

    BranchPythonOperator — returns the task_id of the NEXT task to run.
    If quality checks passed → continue to batch aggregation.
    If quality checks failed → go to alert task instead.

    This is the "taste test" — if the food is bad, don't serve it.
    """
    # Pull results from the quality checks task via XCom
    all_passed = context["ti"].xcom_pull(
        task_ids="run_quality_checks", key="all_passed"
    )

    if all_passed:
        print("Quality gate: PASSED — proceeding to batch aggregation")
        return "run_batch_aggregation"
    else:
        print("Quality gate: FAILED — sending alert")
        return "alert_quality_failure"


def run_batch_aggregation(**context):
    """
    Task 5: Trigger hourly batch aggregation.

    Same as stream processor — actual Spark job runs natively on host.
    """
    print("=" * 50)
    print("BATCH AGGREGATION TASK")
    print("=" * 50)
    print("In production: would trigger spark-submit for batch_etl.py")
    print("Locally: batch ETL runs natively on host machine")
    print("Status: SIMULATED SUCCESS")
    print("=" * 50)
    return "batch_complete"


def update_metadata(**context):
    """
    Task 6: Refresh dataset catalog metadata.

    In the full system, this would update the FastAPI backend's cache
    of table metadata (row counts, last updated times, etc.).
    """
    metadata = {
        "tables_updated": ["clean_events", "dead_letter", "hourly_aggregates"],
        "timestamp": datetime.now().isoformat(),
    }
    print(f"Metadata updated: {json.dumps(metadata, indent=2)}")
    return metadata


def notify_success(**context):
    """
    Task 7: Log successful pipeline completion.

    In production, this would send a Slack message or email.
    """
    dag_run = context.get("dag_run")
    print("=" * 50)
    print("PIPELINE COMPLETE")
    print(f"DAG: {dag_run.dag_id if dag_run else 'unknown'}")
    print(f"Execution date: {context.get('execution_date', 'unknown')}")
    print("All tasks completed successfully!")
    print("=" * 50)


def alert_quality_failure(**context):
    """
    Alert task: triggered when quality checks fail.

    In production, this would page an on-call engineer.
    """
    results = context["ti"].xcom_pull(
        task_ids="run_quality_checks", key="quality_results"
    )
    print("!" * 50)
    print("QUALITY ALERT — Pipeline halted!")
    print(f"Failed checks: {json.dumps(results, indent=2)}")
    print("Action required: investigate data quality issues")
    print("!" * 50)


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="telemetry_pipeline",
    default_args=default_args,
    description="Main telemetry data pipeline: ingest → process → validate → aggregate",
    # Schedule: every 15 minutes
    schedule_interval=timedelta(minutes=15),
    # Don't backfill old runs when the DAG is first enabled
    start_date=datetime(2026, 4, 1),
    catchup=False,
    # Tags help filter DAGs in the Airflow UI
    tags=["telemetry", "pipeline", "data-quality"],
) as dag:

    # --- Define tasks ---

    t1_check_kafka = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )

    t2_stream = PythonOperator(
        task_id="run_stream_processor",
        python_callable=run_stream_processor,
    )

    t3_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    # BranchPythonOperator picks which downstream task to run
    t4_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    t5_batch = PythonOperator(
        task_id="run_batch_aggregation",
        python_callable=run_batch_aggregation,
    )

    t6_metadata = PythonOperator(
        task_id="update_metadata",
        python_callable=update_metadata,
    )

    t7_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        # trigger_rule="none_failed" means: run if no upstream task failed
        # (needed because BranchPythonOperator skips some tasks)
        trigger_rule="none_failed",
    )

    t_alert = PythonOperator(
        task_id="alert_quality_failure",
        python_callable=alert_quality_failure,
    )

    # --- Define task dependencies (the DAG shape) ---
    # Read this as: "after check_kafka, run stream_processor, then quality_checks..."
    t1_check_kafka >> t2_stream >> t3_quality >> t4_gate

    # Quality gate branches:
    t4_gate >> t5_batch >> t6_metadata >> t7_success  # Happy path
    t4_gate >> t_alert                                  # Failure path
