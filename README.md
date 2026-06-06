# kafka-rides

A hands-on Kafka project simulating a ride-hailing event stream. A producer continuously generates fake ride events and publishes them to a Kafka topic; a consumer reads and prints each event in real time.

---

## Architecture

```
┌──────────────┐        rides topic        ┌──────────────┐
│  producer.py │ ────────────────────────► │  consumer.py │
└──────────────┘                           └──────────────┘
                              │
                    ┌─────────▼──────────┐
                    │   Kafka Broker     │
                    │   (KRaft mode)     │
                    │   localhost:9092   │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Schema Registry   │
                    │  localhost:8081    │
                    └────────────────────┘
```

Both the Kafka broker and Schema Registry run in Docker via `docker-compose.yml`. The producer and consumer run locally as Python scripts.

---

## Stack

| Component | Version |
|-----------|---------|
| Kafka broker | Confluent Platform 8.2.0 (KRaft — no Zookeeper) |
| Schema Registry | Confluent Platform 8.2.0 |
| Python client | `confluent-kafka` 2.14.2 |
| Python | 3.x |

---

## Project Structure

```
kafka-rides/
├── docker-compose.yml   # Kafka broker + Schema Registry
├── producer.py          # Generates and publishes ride events
├── consumer.py          # Reads and prints ride events
├── requirements.txt
└── README.md
```

---

## Getting Started

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts:
- **Kafka broker** on `localhost:9092` (KRaft mode — no Zookeeper required)
- **Schema Registry** on `localhost:8081`

### 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the producer

```bash
python producer.py
```

Publishes one ride event per second to the `rides` topic. Output:

```
Producing to topic 'rides' on localhost:9092. Ctrl+C to stop.

Delivered to rides [0] offset 0
Delivered to rides [0] offset 1
...
```

### 4. Run the consumer (separate terminal)

```bash
python consumer.py
```

Reads from the earliest available offset and prints each event:

```
Consuming from topic 'rides' on localhost:9092. Ctrl+C to stop.

[0:0] key=ride_42371 | ride_id=ride_42371 driver=driver_3 status=in_progress fare=$34.50
[0:1] key=ride_81204 | ride_id=ride_81204 driver=driver_1 status=completed fare=$12.00
...
```

---

## Ride Event Schema

Each message is a JSON object keyed by `ride_id`:

```json
{
  "ride_id": "ride_42371",
  "driver_id": "driver_3",
  "status": "in_progress",
  "pickup":  { "lat": 37.762341, "lon": -122.431205 },
  "dropoff": { "lat": 37.791002, "lon": -122.448763 },
  "fare_usd": 34.50,
  "timestamp": "2026-06-06T14:23:01.123456+00:00"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ride_id` | string | Also used as the Kafka message key |
| `driver_id` | string | One of `driver_1` – `driver_5` |
| `status` | string | `requested`, `accepted`, `in_progress`, `completed`, `cancelled` |
| `pickup` / `dropoff` | object | Lat/lon within San Francisco bounds |
| `fare_usd` | float | Random value between $5 and $80 |
| `timestamp` | ISO 8601 | UTC timestamp of event generation |

---

## Kafka Configuration

**Producer**
- `acks=1` — leader acknowledgement only (balance between throughput and durability)

**Consumer**
- `group.id=rides-consumer-group`
- `auto.offset.reset=earliest` — reads from the beginning of the topic on first run

---

## Stopping

Press `Ctrl+C` in either terminal. To stop Docker:

```bash
docker compose down
```

---

## What's Next (Stage 2 ideas)

- **Avro schemas** — enforce a strict schema via Schema Registry (already running)
- **Partitioning by driver** — key messages by `driver_id` so each driver's events land on the same partition, enabling ordered per-driver processing
- **Stream processor** — aggregate per-driver fares or filter rides by status
- **Dead-letter topic** — route malformed messages to a separate topic for inspection
