"""
test_consumer.py — Reads events from Kafka to verify the producer is working.

This is a simple test script that connects to the 'telemetry.raw' topic and
prints out any messages it finds. It reads from the beginning of the topic
(not just new messages) so you can see events that were sent earlier.

Usage:
    source .venv/bin/activate
    python ingestion/test_consumer.py

Press Ctrl+C to stop.
"""

import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "telemetry.raw"


def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    print(f"Reading from topic '{TOPIC}' (from the beginning)...\n")

    # Create a consumer that reads from the very start of the topic
    # auto_offset_reset='earliest' — start from the first message ever sent
    # consumer_timeout_ms=10000 — stop after 10 seconds of no new messages
    # value_deserializer — converts bytes back into a Python dict
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    count = 0
    anomaly_count = 0

    try:
        for message in consumer:
            count += 1
            event = message.value

            # Check if this event looks anomalous
            is_anomaly = (
                event.get("value") is None
                or (isinstance(event.get("value"), (int, float)) and event["value"] > 1000)
                or (isinstance(event.get("value"), (int, float)) and event["value"] < 0)
                or event.get("metric_name", "").startswith("unknown")
                or event.get("timestamp", "").startswith("2099")
            )

            if is_anomaly:
                anomaly_count += 1
                label = "** ANOMALY **"
            else:
                label = "   normal   "

            print(
                f"[{count}] {label} | "
                f"{event.get('device_id', '???')} | "
                f"{event.get('metric_name', '???'):20s} | "
                f"value={event.get('value')}"
            )

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\nRead {count} events total.")
    print(f"Anomalies detected: {anomaly_count} ({anomaly_count/max(count,1)*100:.1f}%)")


if __name__ == "__main__":
    main()
