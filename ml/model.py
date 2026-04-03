"""
model.py — PyTorch autoencoder for anomaly detection.

Architecture:
    Encoder: Input(5) → 64 → 32 → 16 (bottleneck / latent space)
    Decoder: 16 → 32 → 64 → Output(5)

The bottleneck (16 neurons) forces the network to learn a compressed
representation of "normal" telemetry. If new data can't be well-compressed
and reconstructed, it's likely anomalous.

Think of it like this:
    - Normal data: compress well → reconstruct well → LOW error
    - Anomalous data: compress poorly → reconstruct poorly → HIGH error

We use ReLU activations (fast, simple) and dropout (prevents overfitting
on our small dataset).
"""

import torch
import torch.nn as nn


class TelemetryAutoencoder(nn.Module):
    """
    Autoencoder neural network for telemetry anomaly detection.

    The encoder compresses 5 metric features into a 16-dimensional latent space.
    The decoder reconstructs the original 5 features from the latent representation.

    Args:
        input_dim: Number of input features (default: 5 — our 5 metrics)
        dropout_rate: Probability of dropping neurons during training (regularization)
    """

    def __init__(self, input_dim=5, dropout_rate=0.2):
        super().__init__()  # Initialize the parent nn.Module class

        # --- ENCODER: compresses input down to the bottleneck ---
        # Each layer has fewer neurons, forcing the network to learn
        # only the most important patterns (like summarizing a book)
        self.encoder = nn.Sequential(
            # Layer 1: 5 → 64 (expand first to capture complex interactions)
            nn.Linear(input_dim, 64),
            nn.ReLU(),               # Activation: output = max(0, x)
            nn.Dropout(dropout_rate), # Randomly zero out 20% of neurons (prevents overfitting)

            # Layer 2: 64 → 32 (start compressing)
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(dropout_rate),

            # Layer 3: 32 → 16 (the bottleneck — compressed representation)
            nn.Linear(32, 16),
            nn.ReLU(),
        )

        # --- DECODER: reconstructs the original input from the bottleneck ---
        # Mirror of the encoder, expanding back to the original dimension
        self.decoder = nn.Sequential(
            # Layer 1: 16 → 32 (start expanding)
            nn.Linear(16, 32),
            nn.ReLU(),
            nn.Dropout(dropout_rate),

            # Layer 2: 32 → 64 (expand more)
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Dropout(dropout_rate),

            # Layer 3: 64 → 5 (reconstruct original features)
            # No activation here — we want the raw reconstructed values
            # (they're already normalized to [0,1] by the scaler)
            nn.Linear(64, input_dim),
            nn.Sigmoid(),  # Sigmoid squashes output to [0,1] to match normalized input
        )

    def forward(self, x):
        """
        Forward pass: encode then decode.

        Args:
            x: Input tensor of shape (batch_size, input_dim)

        Returns:
            Reconstructed tensor of same shape as input
        """
        # Compress the input to the latent representation
        latent = self.encoder(x)
        # Reconstruct the input from the latent representation
        reconstructed = self.decoder(latent)
        return reconstructed

    def encode(self, x):
        """Get just the latent representation (useful for analysis)."""
        return self.encoder(x)


def count_parameters(model):
    """Count total trainable parameters in the model."""
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


if __name__ == "__main__":
    # Quick test: create model, pass dummy data through it
    model = TelemetryAutoencoder(input_dim=5)
    print(f"Model architecture:\n{model}")
    print(f"\nTotal trainable parameters: {count_parameters(model):,}")

    # Test forward pass with random data
    dummy_input = torch.randn(4, 5)  # Batch of 4 samples, 5 features each
    output = model(dummy_input)
    print(f"\nInput shape:  {dummy_input.shape}")
    print(f"Output shape: {output.shape}")
    print("Forward pass successful!")
