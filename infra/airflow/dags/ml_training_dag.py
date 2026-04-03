"""
ml_training_dag.py — ML training pipeline DAG.

Orchestrates the full ML training workflow:
1. Snapshot the current Iceberg dataset (for reproducibility)
2. Prepare training data (feature engineering, train/test split)
3. Train the autoencoder model
4. Evaluate the model (precision, recall, F1)
5. Register the model artifact + metadata
6. Generate an evaluation report

Schedule: daily at midnight, or triggered manually.

This DAG defines the orchestration — the actual ML code lives in
ml/train.py, ml/evaluate.py, etc. (built in Phase 5).

Task flow:
    snapshot_dataset → prepare_training_data → train_model → evaluate_model
        → register_model → generate_report
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def snapshot_dataset(**context):
    """
    Task 1: Record the current Iceberg snapshot ID.

    Iceberg creates a snapshot every time data is written. By recording
    the snapshot ID before training, we can always reproduce exactly which
    data was used — even if new data arrives during training.

    This is CRITICAL for ML reproducibility.
    """
    # In Phase 5, this will query Iceberg for the latest snapshot ID
    snapshot_id = "placeholder_snapshot_id"

    context["ti"].xcom_push(key="snapshot_id", value=snapshot_id)
    print(f"Dataset snapshot recorded: {snapshot_id}")
    print("This ensures training data is reproducible — even if new data arrives later.")
    return snapshot_id


def prepare_training_data(**context):
    """
    Task 2: Export and prepare training data from the snapshot.

    Steps (implemented in Phase 5):
    - Load clean_events from the specific Iceberg snapshot
    - Pivot: rows = (device_id, time_window), columns = metric values
    - Normalize features to [0, 1]
    - Split by time into train/validation/test sets
    """
    snapshot_id = context["ti"].xcom_pull(
        task_ids="snapshot_dataset", key="snapshot_id"
    )
    print(f"Preparing training data from snapshot: {snapshot_id}")
    print("Steps: load → pivot → normalize → time-based split")
    print("Status: SIMULATED (actual code in Phase 5)")

    # Pass data info downstream
    data_info = {
        "snapshot_id": snapshot_id,
        "num_features": 5,
        "train_size": 0,
        "val_size": 0,
        "test_size": 0,
    }
    context["ti"].xcom_push(key="data_info", value=data_info)
    return data_info


def train_model(**context):
    """
    Task 3: Train the PyTorch autoencoder.

    In Phase 5, this will:
    - Load the prepared dataset
    - Initialize the autoencoder model
    - Run training loop with early stopping
    - Use MPS (Apple GPU) for acceleration
    - Save checkpoint to experiments/ directory
    """
    data_info = context["ti"].xcom_pull(
        task_ids="prepare_training_data", key="data_info"
    )
    experiment_id = f"exp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    print(f"Training autoencoder model — experiment: {experiment_id}")
    print(f"Data snapshot: {data_info.get('snapshot_id', 'unknown')}")
    print("Hyperparams: epochs=50, lr=1e-3, patience=5, device=MPS")
    print("Status: SIMULATED (actual training in Phase 5)")

    training_result = {
        "experiment_id": experiment_id,
        "best_val_loss": 0.0,
        "epochs_completed": 0,
        "device": "mps",
    }
    context["ti"].xcom_push(key="training_result", value=training_result)
    return training_result


def evaluate_model(**context):
    """
    Task 4: Evaluate the trained model.

    In Phase 5, this will:
    - Load the trained model checkpoint
    - Compute reconstruction error on test set
    - Set anomaly threshold at 95th percentile
    - Calculate precision, recall, F1 using injected anomalies as ground truth
    """
    training_result = context["ti"].xcom_pull(
        task_ids="train_model", key="training_result"
    )
    experiment_id = training_result.get("experiment_id", "unknown")

    print(f"Evaluating model from experiment: {experiment_id}")
    print("Metrics: precision, recall, F1, confusion matrix")
    print("Status: SIMULATED (actual evaluation in Phase 5)")

    eval_result = {
        "experiment_id": experiment_id,
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
        "threshold": 0.0,
    }
    context["ti"].xcom_push(key="eval_result", value=eval_result)
    return eval_result


def register_model(**context):
    """
    Task 5: Save the model artifact and metadata.

    Saves the model checkpoint, hyperparameters, evaluation metrics,
    and links to the training data snapshot for full reproducibility.
    """
    training_result = context["ti"].xcom_pull(
        task_ids="train_model", key="training_result"
    )
    eval_result = context["ti"].xcom_pull(
        task_ids="evaluate_model", key="eval_result"
    )

    experiment_id = training_result.get("experiment_id", "unknown")
    print(f"Registering model for experiment: {experiment_id}")
    print(f"Evaluation metrics: {json.dumps(eval_result, indent=2)}")
    print("Artifacts saved to: experiments/{experiment_id}/")
    print("Status: SIMULATED (actual registration in Phase 5)")


def generate_report(**context):
    """
    Task 6: Create a human-readable evaluation report.

    Generates a JSON report with:
    - Training summary (epochs, loss curve, time)
    - Evaluation metrics
    - Dataset snapshot link
    - Comparison with previous experiments
    """
    eval_result = context["ti"].xcom_pull(
        task_ids="evaluate_model", key="eval_result"
    )

    report = {
        "experiment_id": eval_result.get("experiment_id", "unknown"),
        "metrics": eval_result,
        "generated_at": datetime.now().isoformat(),
        "status": "simulated",
    }
    print(f"Evaluation report generated:")
    print(json.dumps(report, indent=2))
    print("ML training pipeline complete!")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="ML model training: snapshot → prepare → train → evaluate → register",
    # Schedule: daily at midnight (or trigger manually)
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ml", "training", "anomaly-detection"],
) as dag:

    t1_snapshot = PythonOperator(
        task_id="snapshot_dataset",
        python_callable=snapshot_dataset,
    )

    t2_prepare = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
    )

    t3_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    t4_evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    t5_register = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
    )

    t6_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Linear chain — each task depends on the previous one
    t1_snapshot >> t2_prepare >> t3_train >> t4_evaluate >> t5_register >> t6_report
