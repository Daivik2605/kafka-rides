import json
from confluent_kafka import Consumer, KafkaError

TOPIC = "rides"
BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "rides-consumer-group",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([TOPIC])

    print(f"Consuming from topic '{TOPIC}' on {BOOTSTRAP_SERVERS}. Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition {msg.partition()} at offset {msg.offset()}")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue

            ride = json.loads(msg.value().decode("utf-8"))
            print(
                f"[{msg.partition()}:{msg.offset()}] "
                f"key={msg.key().decode()} | "
                f"ride_id={ride['ride_id']} driver={ride['driver_id']} "
                f"status={ride['status']} fare=${ride['fare_usd']}"
            )
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Done.")


if __name__ == "__main__":
    main()
