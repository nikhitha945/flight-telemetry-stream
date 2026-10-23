# Flight Telemetry Stream
A real-time telemetry streaming pipeline built with Java 21, Spring Boot, and Apache Kafka.
Simulated aircraft continuously emit telemetry — position, altitude, speed, battery, motor temperature — which is published to Kafka, keyed and partitioned for ordered, parallel processing. This project is a hands-on study of event streaming fundamentals: partitioning, ordering guarantees, consumer groups, offsets, and durability.
Built as a learning project to understand how real-time data platforms ingest and process high-frequency telemetry (the same shape of problem as live aircraft tracking: many producers, continuous events, consumers that cannot afford to fall behind).
Architecture — Day 1
flowchart LR
P["Telemetry Producer<br/>Spring Boot simulator<br/>5 aircraft, 1 event/sec each"]
K[("Kafka (KRaft mode)<br/>topic: telemetry.raw<br/>3 partitions")]
C["Console consumer /<br/>stream processor (Day 2)"]
P -->|"key: aircraftId, value: event JSON"| K
K --> C
Tech stack
Java 21 · Spring Boot 4.1.1 · Spring Kafka
Apache Kafka 4.2.1 in KRaft mode (no Zookeeper), run via Docker Compose
JSON serialization (Spring Kafka JsonSerializer)
Maven
The event
Each reading is an immutable Java record — an event is a fact about a moment, and facts don't change after they happen:
public record TelemetryEvent(
String aircraftId, long eventTime,
double latitude, double longitude,
double altitudeFt, double speedKts,
double batteryPct, double motorTempC) {}
{
"aircraftId": "AC-101",
"eventTime": 1761170400000,
"latitude": 37.3421,
"longitude": -121.8812,
"altitudeFt": 1240.5,
"speedKts": 85.0,
"batteryPct": 91.24,
"motorTempC": 69.8
}
Run it
Prerequisites: Docker, Java 21.
# 1. Start Kafka
docker compose up -d
docker logs kafka          # wait for "Kafka Server started"

# 2. Create the topic (3 partitions)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create --topic telemetry.raw --partitions 3 --replication-factor 1

# 3. Run the producer
./mvnw spring-boot:run

# 4. Watch events arrive in Kafka (separate terminal)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic telemetry.raw \
--from-beginning \
--property print.key=true \
--property print.partition=true

# Design decisions
Topic named by pipeline stage — telemetry.raw.
The .raw suffix means exactly as produced, unmodified — the source of truth for the pipeline. Derived data (aggregates, alerts) will live on separate topics computed from raw, so any processing bug can be fixed and the results recomputed by replaying the untouched original.
Events keyed by aircraft ID.
Kafka routes messages by hash(key) mod partitions, so all events for one aircraft always land in the same partition, in order. Ordering in Kafka is guaranteed only within a partition — keying by aircraft is what preserves the one ordering that matters (per aircraft) inside an otherwise parallel design.
3 partitions, not 1.
A partition is Kafka's unit of parallelism: within a consumer group, one partition can be read by only one consumer at a time. One partition would cap all future processing at a single consumer. Three partitions allow up to three consumers to process in parallel, at the cost of giving up global ordering across aircraft (which this domain doesn't need).
Independent consumer groups.
Reading a topic doesn't consume it destructively. Each consumer group tracks its own progress as committed offsets per partition, so the console consumer, a stream processor, and a future analytics job can all read the same topic at their own pace without interfering.
Durability via acknowledgements + retention, not consumption.
The producer waits for the broker to persist each record (acks=all) before logging it sent. Acknowledged events survive the producer being killed; they are removed only when the topic's retention period expires — never because someone read them.
Verified on Day 1
Producer logs ↔ console consumer output ↔ broker segment files (kafka-dump-log.sh on the partition's .log file) all show the same records.
Key→partition mapping observed directly in storage: AC-101 and AC-102 hashed to partition 0; AC-103 and AC-105 to partition 1 — different keys share partitions, but no key ever appears in two.

# Roadmap
Day 2 — Stream processing: per-aircraft windowed aggregates (1-minute tumbling windows: average speed, motor temperature).
Day 3 — Anomaly detection (rapid battery drain, motor over-temperature) published to a telemetry.alerts topic; latest aircraft state in Redis; REST API serving latest state and aggregates.
Stretch — Live map UI plotting aircraft positions in real time.
tion.yaml            # Kafka bootstrap + serializers