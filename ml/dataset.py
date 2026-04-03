"""
dataset.py — Load telemetry data from Iceberg and prepare it for ML training.

This module handles the full data preparation pipeline:
1. Connect to Iceberg and load clean_events (optionally from a specific snapshot)
2. Pivot: rows = (device_id, time_window), columns = metric values
3. Normalize all features to [0, 1] range
4. Split by TIME (not random) into train / validation / test sets
5. Return PyTorch DataLoaders ready for the autoencoder

Why pivot? Neural networks need fixed-size numeric inputs. Raw events are
one-metric-per-row. After pivoting, each row has ALL metrics for one device
at one time — a 5-number "fingerprint" of device health.

Why time-based split? If we randomly split time series data, the model could
"see the future" during training. Chronological splits prevent this leak.
"""

import os
import sys

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import MinMaxScaler
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The 5 metrics our model expects — one column per metric after pivoting
METRIC_COLUMNS = ["cpu_usage", "memory_usage", "temperature", "disk_io", "network_latency"]


def load_from_iceberg(snapshot_id=None):
    """
    Load clean_events from Iceberg into a Pandas DataFrame.

    Args:
        snapshot_id: If provided, load data from this specific Iceberg snapshot
                     (for reproducibility). If None, load latest data.

    Returns:
        Pandas DataFrame with columns: timestamp, device_id, metric_name, value
    """
    from storage.catalog import get_spark_session, DATABASE_NAME

    spark = get_spark_session("DatasetLoader")

    if snapshot_id:
        # Time-travel query — load data as it existed at a specific snapshot
        query = f"SELECT timestamp, device_id, metric_name, value FROM {DATABASE_NAME}.clean_events VERSION AS OF {snapshot_id}"
        print(f"Loading data from snapshot {snapshot_id}...")
    else:
        query = f"SELECT timestamp, device_id, metric_name, value FROM {DATABASE_NAME}.clean_events"
        print("Loading latest data from clean_events...")

    spark_df = spark.sql(query)
    pdf = spark_df.toPandas()
    spark.stop()

    print(f"Loaded {len(pdf)} events from Iceberg.")
    return pdf


def pivot_and_engineer(df):
    """
    Transform raw events into ML-ready feature vectors.

    Raw format (one row per metric reading):
        timestamp | device_id  | metric_name     | value
        12:00:05  | device_001 | cpu_usage        | 67.3
        12:00:05  | device_001 | memory_usage     | 45.1

    After pivoting (one row per time window, aggregated across ALL devices):
        time_window | cpu_usage | memory_usage | temperature | disk_io | network_latency
        12:00       | 52.1      | 47.3         | 68.2        | 185.4   | 42.7

    Each row is a 5-feature "system health snapshot" for that time window.
    We aggregate across all devices (mean) because our dataset is sparse —
    individual devices don't have all 5 metrics in every window.
    """
    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Create 1-minute time windows (fine-grained for our small dataset)
    # floor('1min') rounds 12:00:37 → 12:00:00, etc.
    df["time_window"] = df["timestamp"].dt.floor("1min")

    # Pivot: one row per time_window, aggregated across ALL devices
    # This gives us a "fleet-wide" health snapshot per minute
    pivoted = df.pivot_table(
        index=["time_window"],
        columns="metric_name",
        values="value",
        aggfunc="mean",
    ).reset_index()

    # Fill missing metrics with column mean (some windows may lack a metric)
    # This is acceptable because the autoencoder learns the mean as "normal"
    for col in METRIC_COLUMNS:
        if col in pivoted.columns:
            pivoted[col] = pivoted[col].fillna(pivoted[col].mean())
        else:
            pivoted[col] = 0.0  # If a metric never appeared, fill with 0

    # Drop rows where we still have NaN (shouldn't happen after fillna, but safe)
    pivoted = pivoted.dropna(subset=METRIC_COLUMNS)

    print(f"Pivoted to {len(pivoted)} feature vectors ({len(METRIC_COLUMNS)} features each).")
    return pivoted


def normalize_features(df, scaler=None):
    """
    Normalize all feature columns to [0, 1] range.

    Why normalize? Neural networks work best when all inputs are on a similar
    scale. CPU usage (0-100) and network latency (0.5-200) have very different
    ranges. MinMaxScaler maps everything to [0, 1].

    Args:
        df: DataFrame with metric columns
        scaler: Pre-fitted scaler (for test/inference). If None, fit a new one.

    Returns:
        (features_array, scaler) — numpy array of normalized features + the scaler
    """
    features = df[METRIC_COLUMNS].values  # Extract just the numeric columns

    if scaler is None:
        scaler = MinMaxScaler(feature_range=(0, 1))
        features_normalized = scaler.fit_transform(features)
        print(f"Fitted new scaler on {len(features)} samples.")
    else:
        features_normalized = scaler.transform(features)
        print(f"Applied existing scaler to {len(features)} samples.")

    return features_normalized, scaler


def time_based_split(df, features, train_ratio=0.7, val_ratio=0.15):
    """
    Split data chronologically: early data for training, later for testing.

    Why not random split? In time series, random splits let the model "see
    the future" during training. Chronological splits simulate reality:
    train on historical data, predict on new data.

    Split: [----train (70%)----][--val (15%)--][--test (15%)--]
                  oldest ──────────────────────────── newest
    """
    # Sort by time_window to ensure chronological order
    sorted_indices = df["time_window"].argsort().values
    features_sorted = features[sorted_indices]

    n = len(features_sorted)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_data = features_sorted[:train_end]
    val_data = features_sorted[train_end:val_end]
    test_data = features_sorted[val_end:]

    print(f"Split: train={len(train_data)}, val={len(val_data)}, test={len(test_data)}")
    return train_data, val_data, test_data


def create_dataloaders(train_data, val_data, test_data, batch_size=32):
    """
    Wrap numpy arrays into PyTorch DataLoaders.

    A DataLoader handles batching (grouping samples), shuffling (randomizing
    order within each epoch), and feeding data to the model efficiently.
    """
    def to_loader(data, shuffle=False):
        tensor = torch.FloatTensor(data)
        dataset = TensorDataset(tensor, tensor)  # Input = target (autoencoder)
        return DataLoader(dataset, batch_size=batch_size, shuffle=shuffle)

    train_loader = to_loader(train_data, shuffle=True)   # Shuffle training data
    val_loader = to_loader(val_data, shuffle=False)       # Don't shuffle val/test
    test_loader = to_loader(test_data, shuffle=False)

    print(f"DataLoaders ready: batch_size={batch_size}")
    return train_loader, val_loader, test_loader


def prepare_dataset(snapshot_id=None, batch_size=32):
    """
    Full pipeline: load → pivot → normalize → split → DataLoaders.

    This is the main entry point called by train.py.

    Returns:
        dict with: train_loader, val_loader, test_loader, scaler, num_features,
                   snapshot_id, and split sizes for experiment tracking.
    """
    # Step 1: Load raw events from Iceberg
    raw_df = load_from_iceberg(snapshot_id=snapshot_id)

    if len(raw_df) == 0:
        raise ValueError("No data found in clean_events. Run the stream processor first.")

    # Step 2: Pivot to feature vectors
    pivoted_df = pivot_and_engineer(raw_df)

    if len(pivoted_df) < 5:
        raise ValueError(
            f"Only {len(pivoted_df)} complete feature vectors after pivoting. "
            "Need more data — run fake_producer.py longer, then stream_processor.py."
        )

    # Step 3: Normalize features
    features, scaler = normalize_features(pivoted_df)

    # Step 4: Time-based split
    train_data, val_data, test_data = time_based_split(pivoted_df, features)

    # Step 5: Create DataLoaders
    train_loader, val_loader, test_loader = create_dataloaders(
        train_data, val_data, test_data, batch_size=batch_size
    )

    # Save scaler parameters for later use in inference
    scaler_params = {
        "min": scaler.data_min_.tolist(),
        "max": scaler.data_max_.tolist(),
        "feature_names": METRIC_COLUMNS,
    }

    return {
        "train_loader": train_loader,
        "val_loader": val_loader,
        "test_loader": test_loader,
        "scaler": scaler,
        "scaler_params": scaler_params,
        "num_features": len(METRIC_COLUMNS),
        "train_size": len(train_data),
        "val_size": len(val_data),
        "test_size": len(test_data),
    }


if __name__ == "__main__":
    # Quick test: load data and show shapes
    print("Testing dataset pipeline...")
    result = prepare_dataset()
    print(f"\nDataset ready:")
    print(f"  Features: {result['num_features']}")
    print(f"  Train: {result['train_size']} samples")
    print(f"  Val: {result['val_size']} samples")
    print(f"  Test: {result['test_size']} samples")
