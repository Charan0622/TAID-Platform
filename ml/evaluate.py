"""
evaluate.py — Evaluate the trained autoencoder and detect anomalies.

Steps:
1. Load the trained model and test dataset
2. Compute reconstruction error for every test sample
3. Set anomaly threshold at the 95th percentile of training errors
4. Calculate metrics: precision, recall, F1
5. Generate a JSON evaluation report

The key insight: normal data reconstructs well (low error), anomalies don't.

Usage:
    python ml/evaluate.py --experiment exp_20260402_225105
    python ml/evaluate.py  # auto-detects latest experiment
"""

import argparse
import json
import os
import sys
from glob import glob

import numpy as np
import torch
import torch.nn as nn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml.model import TelemetryAutoencoder
from ml.dataset import prepare_dataset

EXPERIMENTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments")


def find_latest_experiment():
    """Find the most recent experiment directory."""
    exp_dirs = sorted(glob(os.path.join(EXPERIMENTS_DIR, "exp_*")))
    if not exp_dirs:
        raise FileNotFoundError(f"No experiments found in {EXPERIMENTS_DIR}")
    return os.path.basename(exp_dirs[-1])


def compute_reconstruction_errors(model, dataloader, device):
    """
    Compute per-sample reconstruction error (MSE) for every sample in a dataloader.

    Returns a numpy array of errors, one per sample.
    High error = the model couldn't reconstruct this sample well = potential anomaly.
    """
    model.eval()
    errors = []

    with torch.no_grad():
        for batch_input, _ in dataloader:
            batch_input = batch_input.to(device)
            reconstructed = model(batch_input)

            # Per-sample MSE: average squared error across all features
            # Shape: (batch_size,) — one error value per sample
            batch_errors = torch.mean((batch_input - reconstructed) ** 2, dim=1)
            errors.extend(batch_errors.cpu().numpy())

    return np.array(errors)


def evaluate(experiment_id=None):
    """
    Full evaluation pipeline.

    Args:
        experiment_id: Which experiment to evaluate. None = latest.
    """
    print("=" * 60)
    print("MODEL EVALUATION")
    print("=" * 60)

    # --- Find the experiment ---
    if experiment_id is None:
        experiment_id = find_latest_experiment()
    experiment_dir = os.path.join(EXPERIMENTS_DIR, experiment_id)

    print(f"Evaluating experiment: {experiment_id}")

    # --- Load experiment metadata ---
    log_path = os.path.join(experiment_dir, "experiment.json")
    with open(log_path) as f:
        experiment_log = json.load(f)

    snapshot_id = experiment_log["dataset"].get("snapshot_id")
    input_dim = experiment_log["hyperparameters"]["input_dim"]

    # --- Load model ---
    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    model = TelemetryAutoencoder(input_dim=input_dim)
    model.load_state_dict(torch.load(
        os.path.join(experiment_dir, "model.pt"),
        map_location=device,
        weights_only=True,
    ))
    model = model.to(device)
    model.eval()
    print(f"Model loaded from {experiment_dir}/model.pt")

    # --- Load dataset (same snapshot for reproducibility) ---
    print("\nLoading dataset...")
    data = prepare_dataset(snapshot_id=snapshot_id)

    # --- Compute reconstruction errors ---
    print("\nComputing reconstruction errors...")
    train_errors = compute_reconstruction_errors(model, data["train_loader"], device)
    test_errors = compute_reconstruction_errors(model, data["test_loader"], device)

    # --- Set anomaly threshold ---
    # The threshold is based on training data errors (which should all be "normal")
    # 95th percentile: anything with higher error than 95% of training data = anomaly
    threshold_95 = np.percentile(train_errors, 95)
    threshold_99 = np.percentile(train_errors, 99)

    print(f"\nReconstruction error statistics:")
    print(f"  Training errors:  mean={train_errors.mean():.6f}, std={train_errors.std():.6f}")
    print(f"  Test errors:      mean={test_errors.mean():.6f}, std={test_errors.std():.6f}")
    print(f"  Threshold (95th): {threshold_95:.6f}")
    print(f"  Threshold (99th): {threshold_99:.6f}")

    # --- Classify test samples ---
    # Since our test data comes from clean_events (already validated), there are
    # no "true" anomalies in the test set. We evaluate the model's ability to
    # produce low errors on normal data and high errors on synthetic anomalies.

    # Flag test samples above threshold as "predicted anomalies"
    predicted_anomalies_95 = test_errors > threshold_95
    predicted_anomalies_99 = test_errors > threshold_99

    # For our learning build, we generate synthetic anomalies to test against
    # Create "fake anomaly" samples by randomly perturbing normal data
    np.random.seed(42)
    num_synthetic = max(len(test_errors), 5)
    synthetic_anomalies = np.random.uniform(0, 1, (num_synthetic, input_dim)).astype(np.float32)
    # Make them clearly abnormal: set some values to extreme ranges
    synthetic_anomalies[:, 0] = np.random.uniform(0.9, 1.0, num_synthetic)  # Very high CPU
    synthetic_anomalies[:, 2] = np.random.uniform(0.9, 1.0, num_synthetic)  # Very high temp

    synthetic_tensor = torch.FloatTensor(synthetic_anomalies).to(device)
    with torch.no_grad():
        synthetic_reconstructed = model(synthetic_tensor)
        synthetic_errors = torch.mean(
            (synthetic_tensor - synthetic_reconstructed) ** 2, dim=1
        ).cpu().numpy()

    synthetic_flagged_95 = synthetic_errors > threshold_95
    synthetic_flagged_99 = synthetic_errors > threshold_99

    # --- Compute metrics ---
    # True positives: synthetic anomalies correctly flagged
    # False positives: normal test data incorrectly flagged
    # False negatives: synthetic anomalies missed
    tp = synthetic_flagged_95.sum()
    fp = predicted_anomalies_95.sum()
    fn = num_synthetic - tp
    tn = len(test_errors) - fp

    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    f1 = 2 * precision * recall / max(precision + recall, 1e-8)

    print(f"\nAnomaly detection metrics (95th percentile threshold):")
    print(f"  Synthetic anomalies tested: {num_synthetic}")
    print(f"  True positives (anomalies caught): {tp}")
    print(f"  False positives (normal flagged):  {fp}")
    print(f"  False negatives (anomalies missed): {fn}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1 Score:  {f1:.4f}")

    # --- Generate evaluation report ---
    eval_report = {
        "experiment_id": experiment_id,
        "evaluation_timestamp": __import__("datetime").datetime.now().isoformat(),
        "thresholds": {
            "p95": float(threshold_95),
            "p99": float(threshold_99),
        },
        "error_statistics": {
            "train_mean": float(train_errors.mean()),
            "train_std": float(train_errors.std()),
            "test_mean": float(test_errors.mean()),
            "test_std": float(test_errors.std()),
        },
        "metrics": {
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
        },
        "confusion_matrix": {
            "true_positives": int(tp),
            "false_positives": int(fp),
            "false_negatives": int(fn),
            "true_negatives": int(tn),
        },
        "dataset": {
            "train_size": data["train_size"],
            "test_size": data["test_size"],
            "synthetic_anomalies": num_synthetic,
        },
    }

    report_path = os.path.join(experiment_dir, "evaluation.json")
    with open(report_path, "w") as f:
        json.dump(eval_report, f, indent=2)

    print(f"\nEvaluation report saved to: {report_path}")
    print("Evaluation complete!")

    return eval_report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate telemetry autoencoder")
    parser.add_argument("--experiment", type=str, default=None, help="Experiment ID to evaluate")
    args = parser.parse_args()

    evaluate(experiment_id=args.experiment)
