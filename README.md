# Kafka Mini Project

This project demonstrates a minimal local Kafka setup (broker-only using KRaft) with a Python producer and consumer. The consumer persists incoming sensor messages into a local SQLite database.

## Repository layout

- [producer.py](producer.py): publishes synthetic sensor messages to the `sensor-data` topic.
- [consumer.py](consumer.py): consumes messages from `sensor-data` and stores them in `warehouse.db` (SQLite).
- [docker-compose.yaml](docker-compose.yaml): runs an Apache Kafka broker in KRaft mode (no ZooKeeper).

## What happens when you run things

1. Start Kafka
   - The `docker-compose.yaml` service `kafka` runs a single Kafka broker listening on `localhost:9092`.

2. Create the `sensor-data` topic (optional)
   - Kafka will auto-create topics by default, but you can explicitly create the topic inside the container:

```powershell
# start the broker
docker-compose up -d
# create topic (Windows PowerShell multiline shown earlier works)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic sensor-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

3. Start the producer

```powershell
# inside a Python venv with dependencies installed
.venv\Scripts\python producer.py
```

The producer sends a small batch of three synthetic sensor readings every second to the `sensor-data` topic.

4. Start the consumer

```powershell
.venv\Scripts\python consumer.py
```

- On startup the consumer creates (if not present) the SQLite database file `warehouse.db` and a table named `sensor_logs`.
- For each consumed message the consumer inserts a row into `sensor_logs` with columns: `sensor_id, val, unit, status, ts`.
- The consumer prints a `Stored: <sensor_id> at <timestamp>` line for each message persisted.

## Important implementation details and troubleshooting notes

- Python package: Use `kafka-python` (pip package name `kafka-python`).
  - There is an older/conflicting package named `kafka` which is incompatible and raises `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`. If you see that error, uninstall `kafka` and install `kafka-python`:

```powershell
.venv\Scripts\python -m pip uninstall -y kafka
.venv\Scripts\python -m pip install kafka-python
```

- Consumer robustness improvements
  - The `consumer.py` includes logic to recreate the `KafkaConsumer` and retry when transient errors occur.
  - In particular, on selector/file-descriptor `ValueError` and other `KafkaError` exceptions the consumer closes and recreates its client, then continues polling. This avoids the process exiting when the underlying selector becomes invalid.
  - `KeyboardInterrupt` is handled to close the consumer and database cleanly.

- SQLite details
  - The DB file created is `warehouse.db` in the project directory. The consumer uses `sqlite3` from the Python standard library.
  - Schema (table `sensor_logs`): `id, sensor_id, val, unit, status, ts`.

## Dependencies

- Python 3.11+ recommended.
- `kafka-python` (install into your venv):

```powershell
.venv\Scripts\python -m pip install kafka-python
```

## Quick verification

- Start Kafka with `docker-compose up -d`.
- Run `producer.py` and `consumer.py` in separate terminals.
- Use `sqlite3` or a DB browser to view `warehouse.db` and confirm rows in `sensor_logs`.

## Next steps / enhancements

- Add a `requirements.txt` for reproducible installs.
- Add graceful shutdown and metrics for processed message counts.
- Add tests that mock Kafka to verify DB writes without a running broker.

