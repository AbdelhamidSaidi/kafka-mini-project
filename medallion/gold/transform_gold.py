import time
import traceback
import argparse
import datetime
try:
    from medallion.silver import silver
    from medallion.silver.silver import get_connection, parse_timestamp
except ModuleNotFoundError:
    # allow running this file directly from medallion/gold by adding project root to sys.path
    import os
    import sys
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from medallion.silver import silver
    from medallion.silver.silver import get_connection, parse_timestamp


DEFAULT_SPECS = {
    'vent': {'min': 15.0, 'max': 30.0},
    'humidifier': {'min': 20.0, 'max': 30.0},
    'thermometer': {'min': 15.0, 'max': 30.0},
}


def ensure_gold_tables(cursor):
    # sensor dimension (authoritative list of sensors)
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_sensor')
    BEGIN
        CREATE TABLE dim_sensor (
            sensor_id NVARCHAR(255) NOT NULL PRIMARY KEY,
            first_seen DATETIME2 NULL,
            last_seen DATETIME2 NULL
        );
    END
    ''')

    # central gold table
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gold_central')
    BEGIN
        CREATE TABLE gold_central (
            id INT IDENTITY(1,1) PRIMARY KEY,
            sensor_id NVARCHAR(255),
            metric NVARCHAR(50),
            value FLOAT,
            unit NVARCHAR(50),
            ts DATETIME2,
            status NVARCHAR(20),
            ingestion_time DATETIME2 DEFAULT SYSUTCDATETIME()
        );
    END
    ''')

    # FK gold_central.sensor_id -> dim_sensor.sensor_id
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_gold_central_dim_sensor')
    BEGIN
        ALTER TABLE gold_central WITH CHECK
        ADD CONSTRAINT FK_gold_central_dim_sensor
        FOREIGN KEY (sensor_id) REFERENCES dim_sensor(sensor_id);
    END
    ''')

    # derived tables: vent, humidifier, thermometer link to central via sensor_id
    for name in ('gold_vent', 'gold_humidifier', 'gold_thermometer'):
        cursor.execute(f'''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{name}')
        BEGIN
            CREATE TABLE {name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                central_id INT,
                sensor_id NVARCHAR(255),
                value FLOAT,
                unit NVARCHAR(50),
                ts DATETIME2,
                status NVARCHAR(20),
                ingestion_time DATETIME2 DEFAULT SYSUTCDATETIME(),
                FOREIGN KEY (central_id) REFERENCES gold_central(id)
            );
        END
        ''')


def ensure_gold_watermark(cursor):
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gold_watermark')
    BEGIN
        CREATE TABLE gold_watermark (
            id INT IDENTITY(1,1) PRIMARY KEY,
            last_ts DATETIME2 NULL
        );
    END
    ''')


def ensure_dimension_tables(cursor):
    # dimensions removed per user request — function kept as a no-op placeholder
    return


def get_gold_watermark(cursor):
    cursor.execute('SELECT TOP 1 last_ts FROM gold_watermark ORDER BY id DESC')
    r = cursor.fetchone()
    return r[0] if r and r[0] is not None else None


def set_gold_watermark(cursor, ts):
    cursor.execute('DELETE FROM gold_watermark')
    cursor.execute('INSERT INTO gold_watermark (last_ts) VALUES (?)', (ts,))


def evaluate_status(metric_name, value):
    specs = DEFAULT_SPECS.get(metric_name, None)
    if specs is None or value is None:
        return 'unknown'
    if value < specs['min']:
        return 'idle'
    if value > specs['max']:
        return 'warning'
    return 'ok'


def build_gold_layer(poll_interval_seconds=1):
    conn = get_connection()
    cur = conn.cursor()
    try:
        ensure_gold_tables(cur)
        ensure_gold_watermark(cur)
        ensure_dimension_tables(cur)
        conn.commit()

        # streaming loop: read latest rows from sensor_logs_silver and insert to gold
        # initialize watermark for gold layer
        last_ts = get_gold_watermark(cur)
        if last_ts is None:
            # fall back to latest gold_central ts if watermark empty
            cur.execute('SELECT MAX(ts) FROM gold_central')
            r = cur.fetchone()
            if r and r[0] is not None:
                last_ts = r[0]

        print('Starting gold layer stream, last_ts=', last_ts)

        while True:
            try:
                # select new silver rows
                if last_ts is None:
                    cur.execute('SELECT id, sensor_id, value, unit, ts FROM sensor_logs_silver ORDER BY ts')
                else:
                    cur.execute('SELECT id, sensor_id, value, unit, ts FROM sensor_logs_silver WHERE ts > ? ORDER BY ts', (last_ts,))
                rows = cur.fetchall()
                if not rows:
                    time.sleep(poll_interval_seconds)
                    continue

                for row in rows:
                    sid, sensor_id, value, unit, ts = row[0], row[1], row[2], row[3], row[4]
                    # ts may already be datetime; ensure
                    if not isinstance(ts, (str,)):
                        parsed_ts = ts
                    else:
                        parsed_ts, _ = parse_timestamp(ts)

                    # determine metric type from sensor_id naming convention
                    metric = None
                    lid = (sensor_id or '').lower()
                    if 'vent' in lid:
                        metric = 'vent'
                    elif 'humid' in lid:
                        metric = 'humidifier'
                    elif 'therm' in lid or 'thermostat' in lid:
                        metric = 'thermometer'
                    else:
                        metric = 'unknown'

                    status = evaluate_status(metric, value)

                    # ensure sensor exists in dim_sensor before inserting gold_central (required by FK)
                    now = datetime.datetime.now(datetime.timezone.utc)
                    cur.execute(
                        "IF NOT EXISTS (SELECT 1 FROM dim_sensor WHERE sensor_id = ?) "
                        "INSERT INTO dim_sensor (sensor_id, first_seen, last_seen) VALUES (?, ?, ?)",
                        (sensor_id, sensor_id, now, now)
                    )
                    cur.execute(
                        "UPDATE dim_sensor SET last_seen = ? WHERE sensor_id = ?",
                        (now, sensor_id)
                    )

                    # insert into central gold
                    cur.execute(
                        'INSERT INTO gold_central (sensor_id, metric, value, unit, ts, status) VALUES (?, ?, ?, ?, ?, ?)',
                        (sensor_id, metric, value, unit, parsed_ts, status)
                    )
                    # retrieve new central id (pyodbc may require a separate fetch)
                    try:
                        central_id = cur.execute('SELECT SCOPE_IDENTITY()').fetchval()
                    except Exception:
                        # fallback: query last inserted by sensor_id+ts
                        cur.execute('SELECT TOP 1 id FROM gold_central WHERE sensor_id = ? AND ts = ? ORDER BY id DESC', (sensor_id, parsed_ts))
                        rcid = cur.fetchone()
                        central_id = rcid[0] if rcid else None

                    # insert into derived table
                    dest = None
                    if metric == 'vent':
                        dest = 'gold_vent'
                    elif metric == 'humidifier':
                        dest = 'gold_humidifier'
                    elif metric == 'thermometer':
                        dest = 'gold_thermometer'

                    if dest and central_id is not None:
                        cur.execute(
                            f'INSERT INTO {dest} (central_id, sensor_id, value, unit, ts, status) VALUES (?, ?, ?, ?, ?, ?)',
                            (central_id, sensor_id, value, unit, parsed_ts, status)
                        )

                    # dimension upsert removed per config — no-op

                    last_ts = parsed_ts if last_ts is None or parsed_ts > last_ts else last_ts

                # persist watermark for gold layer then commit
                try:
                    if last_ts is not None:
                        set_gold_watermark(cur, last_ts)
                except Exception:
                    pass
                conn.commit()
                print(f'Inserted {len(rows)} rows into gold layer; last_ts={last_ts}')

            except Exception:
                conn.rollback()
                print('Error in gold streaming iteration:')
                traceback.print_exc()
                time.sleep(poll_interval_seconds)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def main():
    p = argparse.ArgumentParser(description='Run gold layer streaming transformer')
    p.add_argument('--interval', type=int, default=1, help='poll interval seconds (default: 1)')
    args = p.parse_args()
    build_gold_layer(poll_interval_seconds=args.interval)


if __name__ == '__main__':
    main()
