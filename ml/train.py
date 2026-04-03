"""
train.py — Training loop for the telemetry autoencoder.

Trains the autoencoder on clean telemetry data, tracking:
- Training and validation loss per epoch
- Best model checkpoint (saved to disk)
- Experiment metadata (hyperparams, snapshot_id, timing)
- MPS (Apple GPU) acceleration when available

Early stopping: if validation loss doesn't improve for `patience` epochs,
training stops early to prevent overfitting.

Usage:
    python ml/train.py
    python ml/train.py --epochs 100 --lr 0.001
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import torch
import torch.nn as nn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml.model import TelemetryAutoencoder, count_parameters
from ml.dataset import prepare_dataset

# Experiments directory — each run gets its own folder
EXPERIMENTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments")


def get_device():
    """
    Pick the best available compute device.

    Priority: MPS (Apple GPU) > CUDA (NVIDIA GPU) > CPU
    On our M4 MacBook, MPS should be available.
    """
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        print(f"Using device: MPS (Apple Silicon GPU)")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        print(f"Using device: CUDA (NVIDIA GPU)")
    else:
        device = torch.device("cpu")
        print(f"Using device: CPU")
    return device


def train_one_epoch(model, dataloader, optimizer, criterion, device):
    """
    Train the model for one epoch (one pass through all training data).

    Returns the average loss for this epoch.
    """
    model.train()  # Enable training mode (activates dropout)
    total_loss = 0.0
    num_batches = 0

    for batch_input, batch_target in dataloader:
        # Move data to the compute device (MPS/CPU)
        batch_input = batch_input.to(device)
        batch_target = batch_target.to(device)

        # Forward pass: run input through the autoencoder
        reconstructed = model(batch_input)

        # Compute loss: how different is the reconstruction from the original?
        loss = criterion(reconstructed, batch_target)

        # Backward pass: compute gradients (how to adjust each weight)
        optimizer.zero_grad()  # Clear old gradients
        loss.backward()        # Compute new gradients
        optimizer.step()       # Update weights using the gradients

        total_loss += loss.item()
        num_batches += 1

    return total_loss / max(num_batches, 1)


def validate(model, dataloader, criterion, device):
    """
    Evaluate the model on validation data (no weight updates).

    Returns the average validation loss.
    """
    model.eval()  # Disable dropout and other training-specific behaviors
    total_loss = 0.0
    num_batches = 0

    with torch.no_grad():  # Disable gradient computation (saves memory, faster)
        for batch_input, batch_target in dataloader:
            batch_input = batch_input.to(device)
            batch_target = batch_target.to(device)

            reconstructed = model(batch_input)
            loss = criterion(reconstructed, batch_target)

            total_loss += loss.item()
            num_batches += 1

    return total_loss / max(num_batches, 1)


def train(epochs=50, lr=1e-3, patience=5, batch_size=32, snapshot_id=None):
    """
    Full training pipeline.

    Args:
        epochs: Maximum number of training epochs
        lr: Learning rate (how big each weight update step is)
        patience: Stop early if val loss doesn't improve for this many epochs
        batch_size: Number of samples per training batch
        snapshot_id: Iceberg snapshot for reproducibility (None = latest data)
    """
    print("=" * 60)
    print("AUTOENCODER TRAINING")
    print("=" * 60)

    # --- Setup ---
    device = get_device()
    experiment_id = f"exp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    experiment_dir = os.path.join(EXPERIMENTS_DIR, experiment_id)
    os.makedirs(experiment_dir, exist_ok=True)

    # --- Load and prepare data ---
    print("\nPreparing dataset...")
    data = prepare_dataset(snapshot_id=snapshot_id, batch_size=batch_size)

    # --- Initialize model ---
    model = TelemetryAutoencoder(input_dim=data["num_features"])
    model = model.to(device)
    print(f"\nModel: {count_parameters(model):,} parameters")

    # --- Loss function and optimizer ---
    # MSE Loss: average of (predicted - actual)^2
    # Perfect reconstruction = 0 loss
    criterion = nn.MSELoss()
    # Adam optimizer: adaptive learning rates, widely used default
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # --- Training loop ---
    print(f"\nTraining for up to {epochs} epochs (early stopping patience={patience})...")
    print(f"{'Epoch':>6} | {'Train Loss':>12} | {'Val Loss':>12} | {'Status':>10}")
    print("-" * 50)

    best_val_loss = float("inf")
    epochs_without_improvement = 0
    train_losses = []
    val_losses = []
    start_time = time.time()

    for epoch in range(1, epochs + 1):
        # Train one epoch
        train_loss = train_one_epoch(model, data["train_loader"], optimizer, criterion, device)
        # Validate
        val_loss = validate(model, data["val_loader"], criterion, device)

        train_losses.append(train_loss)
        val_losses.append(val_loss)

        # Check if this is the best model so far
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            epochs_without_improvement = 0
            # Save the best model checkpoint
            torch.save(model.state_dict(), os.path.join(experiment_dir, "model.pt"))
            status = "* best *"
        else:
            epochs_without_improvement += 1
            status = ""

        # Print progress every 5 epochs (or first/last)
        if epoch % 5 == 0 or epoch == 1 or status:
            print(f"{epoch:>6} | {train_loss:>12.6f} | {val_loss:>12.6f} | {status:>10}")

        # Early stopping
        if epochs_without_improvement >= patience:
            print(f"\nEarly stopping at epoch {epoch} (no improvement for {patience} epochs)")
            break

    training_time = time.time() - start_time

    # --- Save experiment metadata ---
    experiment_log = {
        "experiment_id": experiment_id,
        "timestamp": datetime.now().isoformat(),
        "hyperparameters": {
            "epochs_max": epochs,
            "epochs_completed": epoch,
            "learning_rate": lr,
            "batch_size": batch_size,
            "patience": patience,
            "input_dim": data["num_features"],
            "dropout_rate": 0.2,
        },
        "dataset": {
            "snapshot_id": snapshot_id,
            "train_size": data["train_size"],
            "val_size": data["val_size"],
            "test_size": data["test_size"],
        },
        "results": {
            "best_val_loss": best_val_loss,
            "final_train_loss": train_losses[-1],
            "training_time_seconds": round(training_time, 2),
            "device": str(device),
        },
        "losses": {
            "train": train_losses,
            "val": val_losses,
        },
        "scaler_params": data["scaler_params"],
    }

    log_path = os.path.join(experiment_dir, "experiment.json")
    with open(log_path, "w") as f:
        json.dump(experiment_log, f, indent=2)

    print(f"\nTraining complete!")
    print(f"  Experiment: {experiment_id}")
    print(f"  Best val loss: {best_val_loss:.6f}")
    print(f"  Training time: {training_time:.1f}s")
    print(f"  Device: {device}")
    print(f"  Saved to: {experiment_dir}/")

    return experiment_id, experiment_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train telemetry autoencoder")
    parser.add_argument("--epochs", type=int, default=50, help="Max training epochs")
    parser.add_argument("--lr", type=float, default=1e-3, help="Learning rate")
    parser.add_argument("--patience", type=int, default=5, help="Early stopping patience")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size")
    parser.add_argument("--snapshot-id", type=str, default=None, help="Iceberg snapshot ID")
    args = parser.parse_args()

    train(
        epochs=args.epochs,
        lr=args.lr,
        patience=args.patience,
        batch_size=args.batch_size,
        snapshot_id=args.snapshot_id,
    )
