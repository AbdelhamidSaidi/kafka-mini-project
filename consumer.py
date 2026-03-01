import json
import sqlite3
import time
import traceback
from kafka import KafkaConsumer
from kafka.errors import KafkaError


# 1. Setup SQLite Database
db_connection = sqlite3.connect('warehouse.db', check_same_thread=False)
cursor = db_connection.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id TEXT,
        val REAL,
        unit TEXT,
        ts TEXT
    )
''')
db_connection.commit()


def create_consumer():
    return KafkaConsumer(
        'sensor-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='storage-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def run_consumer():
    print("Consumer linked to SQLite. Writing data...")
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = create_consumer()

            for message in consumer:
                row = message.value
                cursor.execute('''
                    INSERT INTO sensor_logs (sensor_id, val, unit, ts)
                    VALUES (?, ?, ?, ?)
                ''', (row.get('sensor_id'), row.get('value'), row.get('type'), row.get('timestamp')))
                db_connection.commit()
                print(f"Stored: {row.get('sensor_id')} at {row.get('timestamp')}")

        except KeyboardInterrupt:
            print('Interrupted by user, closing...')
            try:
                if consumer:
                    consumer.close()
            finally:
                db_connection.close()
            break
        except ValueError as e:
            # Catch selector / invalid fd errors and restart the consumer
            print('Selector/File descriptor error detected — restarting consumer')
            print(e)
            try:
                if consumer:
                    consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(1)
            continue
        except KafkaError as e:
            print('Kafka error — will retry:', e)
            traceback.print_exc()
            try:
                if consumer:
                    consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(5)
            continue
        except Exception as e:
            print('Unexpected error in consumer loop — restarting in 2s')
            traceback.print_exc()
            try:
                if consumer:
                    consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(2)
            continue


if __name__ == '__main__':
    run_consumer()