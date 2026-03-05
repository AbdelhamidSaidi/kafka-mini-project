import json
import os
import time
import traceback
import pyodbc
from kafka import KafkaConsumer
from kafka.errors import KafkaError


# 1. Setup SQL Server connection (pyodbc)
# The consumer will read connection info from the env var `MSSQL_CONN`.
# If not provided it will prompt you to paste an ODBC connection string, for example:
# DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=yourdb;UID=sa;PWD=yourpassword

# Explicit SQL Server connection variables (edit these with your server info)
# No environment variables or venv required — fill these values directly.
DB_CONFIG = {
    'server': 'HP',
    'database': 'data_warehouse',
    'driver': 'ODBC Driver 17 for SQL Server',
    'trusted_connection': 'yes'
}

def get_mssql_connection():
    # Ensure driver is wrapped in braces
    driver = DB_CONFIG.get('driver', '')
    if not driver.startswith('{'):
        driver = '{' + driver + '}'

    server = DB_CONFIG.get('server')
    port = DB_CONFIG.get('port', 1433)
    database = DB_CONFIG.get('database')
    trusted = str(DB_CONFIG.get('trusted_connection', '')).lower() in ('yes', 'true', '1')
    if trusted:
        conn_str = f"DRIVER={driver};SERVER={server},{port};DATABASE={database};Trusted_Connection=yes;"
    else:
        # If not trusted, expect username/password fields in DB_CONFIG
        uid = DB_CONFIG.get('uid')
        pwd = DB_CONFIG.get('pwd', '')
        conn_str = f"DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={uid};PWD={pwd}"

    # Try multiple candidate connection strings to improve diagnostics
    candidates = [conn_str]
    # Add variants: omit port, use localhost, use 127.0.0.1
    if trusted:
        candidates.extend([
            f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;",
            f"DRIVER={driver};SERVER=localhost,{port};DATABASE={database};Trusted_Connection=yes;",
            f"DRIVER={driver};SERVER=127.0.0.1,{port};DATABASE={database};Trusted_Connection=yes;",
        ])
    else:
        uid = DB_CONFIG.get('uid')
        pwd = DB_CONFIG.get('pwd', '')
        candidates.extend([
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}",
            f"DRIVER={driver};SERVER=localhost,{port};DATABASE={database};UID={uid};PWD={pwd}",
            f"DRIVER={driver};SERVER=127.0.0.1,{port};DATABASE={database};UID={uid};PWD={pwd}",
        ])

    last_err = None
    for c in candidates:
        # Mask password when printing
        display = c
        if 'PWD=' in c:
            display = c.split('PWD=')[0] + 'PWD=***'
        print(f"Trying connection string: {display}")
        try:
            conn = pyodbc.connect(c, timeout=5)
            conn.autocommit = True
            print("Connected to SQL Server successfully.")
            return conn
        except Exception as e:
            last_err = e
            print(f"Connection attempt failed: {e}")
            continue

    # If none succeeded, raise the last error with guidance
    raise RuntimeError(
        "All connection attempts failed. Check that SQL Server is running, TCP/IP is enabled, firewall allows port 1433, and the server name/instance are correct. Last error: "
        + str(last_err)
    )


def ensure_database_exists(database_name):

    if not database_name:
        return

    driver = DB_CONFIG.get('driver', '')
    if not driver.startswith('{'):
        driver = '{' + driver + '}'

    server = DB_CONFIG.get('server')
    port = DB_CONFIG.get('port', 1433)
    trusted = str(DB_CONFIG.get('trusted_connection', '')).lower() in ('yes', 'true', '1')

    if trusted:
        master_conn = f"DRIVER={driver};SERVER={server},{port};DATABASE=master;Trusted_Connection=yes;"
    else:
        uid = DB_CONFIG.get('uid')
        pwd = DB_CONFIG.get('pwd', '')
        master_conn = f"DRIVER={driver};SERVER={server},{port};DATABASE=master;UID={uid};PWD={pwd}"

    try:
        print(f"Ensuring database '{database_name}' exists (connecting to master)...")
        conn = pyodbc.connect(master_conn, timeout=5)
        conn.autocommit = True
        cur = conn.cursor()
        # Use DB_ID check to avoid race/create-if-exists issues
        cur.execute(f"IF DB_ID(N'{database_name}') IS NULL BEGIN CREATE DATABASE [{database_name}] END")
        cur.close()
        conn.close()
        print(f"Database '{database_name}' ensured (or already existed).")
    except Exception as e:
        # Don't raise here; main connection will surface the error if needed.
        print(f"Warning: could not ensure database '{database_name}': {e}")

# Ensure database exists and then ensure table exists
ensure_database_exists(DB_CONFIG.get('database'))
db_connection = get_mssql_connection()
cursor = db_connection.cursor()
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensor_logs')
BEGIN
    CREATE TABLE sensor_logs (
        id INT IDENTITY(1,1) PRIMARY KEY,
        sensor_id NVARCHAR(255),
        val FLOAT,
        unit NVARCHAR(50),
        ts NVARCHAR(50)
    );
END
''')


def create_consumer():
    return KafkaConsumer(
        'sensor-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='storage-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def run_consumer():
    print("Consumer linked to SQL Server. Writing data...")
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = create_consumer()

            for message in consumer:
                row = message.value
                cursor.execute(
                    '''
                    INSERT INTO sensor_logs (sensor_id, val, unit, ts)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (row.get('sensor_id'), row.get('value'), row.get('type'), row.get('timestamp'))
                )
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