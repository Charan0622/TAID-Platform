"""
fake_producer.py — Generates fake telemetry events and sends them to Kafka.

This script pretends to be 50 different devices (sensors, servers, etc.) that are
constantly reporting metrics like CPU usage, temperature, and network latency.
Every 0.5 seconds, it creates a random event and pushes it into the Kafka topic
'telemetry.raw'. About 5% of events are intentionally anomalous (bad data) so
we can test our validation pipeline later in Phase 3.

Usage:
    source .venv/bin/activate
    python ingestion/fake_producer.py

Press Ctrl+C to stop.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKER = "localhost:9092"      # Where Kafka is listening
TOPIC = "telemetry.raw"              # The Kafka topic we send events to
SEND_INTERVAL = 0.5                  # Seconds between events

# Our 50 fake devices — each has a unique ID like "device_001"
DEVICE_IDS = [f"device_{i:03d}" for i in range(1, 51)]

# The 5 types of metrics our "sensors" can report, with realistic value ranges
# Format: metric_name → (min_normal, max_normal, unit)
METRIC_CONFIGS = {
    "cpu_usage":        (0.0,   100.0,  "percent"),
    "memory_usage":     (0.0,   100.0,  "percent"),
    "temperature":      (15.0,  95.0,   "celsius"),
    "disk_io":          (0.0,   500.0,  "mbps"),
    "network_latency":  (0.5,   200.0,  "ms"),
}

# Locations where our fake devices live
LOCATIONS = [
    "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1",
    "building-a-floor-1", "building-a-floor-2", "building-b-floor-1",
    "datacenter-north", "datacenter-south", "edge-site-01",
]

# How often to inject bad data (5% = 1 in 20 events)
ANOMALY_RATE = 0.05


def create_normal_event() -> dict:
    """Generate a single realistic telemetry event with valid data."""
    # Pick a random metric type
    metric_name = random.choice(list(METRIC_CONFIGS.keys()))
    min_val, max_val, unit = METRIC_CONFIGS[metric_name]

    return {
        "event_id": str(uuid.uuid4()),            # Unique ID for deduplication
        "timestamp": datetime.now(timezone.utc).isoformat(),  # Current time in UTC
        "device_id": random.choice(DEVICE_IDS),    # Random device
        "metric_name": metric_name,
        "value": round(random.uniform(min_val, max_val), 2),  # Random value in range
        "unit": unit,
        "location": random.choice(LOCATIONS),
    }


def create_anomalous_event() -> dict:
    """
    Generate a deliberately bad event. These test our validation in Phase 3.
    Randomly picks one of several failure modes that happen in real systems.
    """
    event = create_normal_event()  # Start with a good event, then break it

    # Pick a random way to corrupt the data
    anomaly_type = random.choice([
        "null_value",         # Sensor failed to read — value is missing
        "extreme_value",      # Sensor glitch — wildly out of range
        "future_timestamp",   # Clock drift — timestamp is in the future
        "negative_value",     # Impossible negative reading
        "unknown_metric",     # Typo or misconfigured sensor
    ])

    if anomaly_type == "null_value":
        event["value"] = None                          # Sensor returned nothing

    elif anomaly_type == "extreme_value":
        # 10x the normal maximum — clearly a sensor malfunction
        event["value"] = round(random.uniform(1000, 9999), 2)

    elif anomaly_type == "future_timestamp":
        # Timestamp 1 day in the future — impossible (clock skew)
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        event["timestamp"] = future.isoformat()

    elif anomaly_type == "negative_value":
        event["value"] = round(random.uniform(-500, -1), 2)  # Can't have negative CPU%

    elif anomaly_type == "unknown_metric":
        event["metric_name"] = "unknown_sensor_xyz"    # Not in our allowed list

    return event


def main():
    """Connect to Kafka and start sending events forever."""

    # Create a Kafka producer — this object manages the connection to Kafka
    # value_serializer: converts our Python dict → JSON bytes (Kafka only sends bytes)
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Connected! Sending events to topic '{TOPIC}' every {SEND_INTERVAL}s")
    print("Press Ctrl+C to stop.\n")

    event_count = 0

    try:
        while True:
            # 5% chance of generating a bad event
            if random.random() < ANOMALY_RATE:
                event = create_anomalous_event()
                event_type = "ANOMALY"
            else:
                event = create_normal_event()
                event_type = "normal"

            # Send the event to Kafka
            # .send() is non-blocking — it queues the message and returns immediately
            producer.send(TOPIC, value=event)

            event_count += 1

            # Print a summary so we can see what's happening
            value_display = event.get("value", "null")
            print(
                f"[{event_count}] {event_type:7s} | "
                f"{event['device_id']} | "
                f"{event.get('metric_name', '???'):20s} | "
                f"value={value_display}"
            )

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print(f"\nStopped. Sent {event_count} events total.")
    finally:
        # Flush ensures all queued messages are actually sent before we exit
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
