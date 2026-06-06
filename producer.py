import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "rides"
BOOTSTRAP_SERVERS = "localhost:9092"

DRIVERS = ["driver_1", "driver_2", "driver_3", "driver_4", "driver_5"]
STATUSES = ["requested", "accepted", "in_progress", "completed", "cancelled"]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def generate_ride():
    return {
        "ride_id": f"ride_{random.randint(10000, 99999)}",
        "driver_id": random.choice(DRIVERS),
        "status": random.choice(STATUSES),
        "pickup": {
            "lat": round(random.uniform(37.7, 37.8), 6),
            "lon": round(random.uniform(-122.5, -122.4), 6),
        },
        "dropoff": {
            "lat": round(random.uniform(37.7, 37.8), 6),
            "lon": round(random.uniform(-122.5, -122.4), 6),
        },
        "fare_usd": round(random.uniform(5.0, 80.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "1",
    })

    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}. Ctrl+C to stop.\n")

    try:
        while True:
            ride = generate_ride()
            producer.produce(
                topic=TOPIC,
                key=ride["ride_id"],
                value=json.dumps(ride),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()
