import os
import pyodbc
import datetime
import traceback
import time

# Read DB config from environment when available (use .env or docker-compose)
DB_CONFIG = {
    'server': os.environ.get('MSSQL_SERVER', 'localhost'),
    'port': int(os.environ.get('MSSQL_PORT', 1433)),
    'database': os.environ.get('MSSQL_DATABASE', 'data_warehouse'),
    'driver': os.environ.get('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server'),
    'uid': os.environ.get('MSSQL_UID', 'your_username'),
    'pwd': os.environ.get('MSSQL_PWD', 'your_password'),
    'trusted_connection': os.environ.get('MSSQL_TRUSTED', 'yes')
}

def get_connection():
    server = DB_CONFIG.get('server')
    port = DB_CONFIG.get('port', 1433)
    database = DB_CONFIG.get('database')
    trusted = str(DB_CONFIG.get('trusted_connection', '')).lower() in ('yes', 'true', '1')

    # Discover installed ODBC drivers and prefer SQL Server drivers
    available_drivers = [d for d in pyodbc.drivers()]
    if not available_drivers:
        raise RuntimeError('No ODBC drivers are installed or visible to pyodbc. Install Microsoft ODBC Driver for SQL Server.')

    # Build ordered driver list: user-specified (if installed), then common SQL Server drivers found
    drivers = []
    user_drv = DB_CONFIG.get('driver')
    if user_drv and user_drv in available_drivers:
        drivers.append(user_drv)

    for preferred in ('ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server', 'SQL Server Native Client 11.0', 'SQL Server'):
        if preferred in available_drivers and preferred not in drivers:
            drivers.append(preferred)

    # Finally fall back to the first available driver
    for d in available_drivers:
        if d not in drivers:
            drivers.append(d)

    # Candidate server formats to try
    server_variants = [f"{server},{port}", f"{server}", f"tcp:{server},{port}"]
    # Add common named instances
    common_instances = [f"{server}\\SQLEXPRESS", f"{server}\\MSSQLSERVER"]

    # Warn if database looks like a placeholder
    if not database or database.lower() in ('your_database', 'database'):
        print("Warning: DB_CONFIG['database'] looks unset or placeholder; please set a real database name.")

    last_exc = None
    attempt = 0
    for drv in drivers:
        drv_fmt = drv
        if not drv_fmt.startswith('{'):
            drv_fmt = '{' + drv_fmt + '}'
        base_auth = 'Trusted_Connection=yes;' if trusted else f"UID={DB_CONFIG.get('uid')};PWD={DB_CONFIG.get('pwd','')};"

        for s in server_variants + common_instances:
            attempt += 1
            conn_str = f"DRIVER={drv_fmt};SERVER={s};DATABASE={database};{base_auth}Connection Timeout=5;"
            try:
                conn = pyodbc.connect(conn_str)
                print(f"Connection attempt {attempt} succeeded using driver='{drv}' server='{s}'")
                return conn
            except Exception as e:
                last_exc = e
                try:
                    if isinstance(e.args, tuple) and len(e.args) >= 2:
                        code = e.args[0]
                        detail = str(e.args[1]).splitlines()[0]
                        msg = f"{code}: {detail}"
                    else:
                        msg = str(e).splitlines()[0]
                except Exception:
                    msg = repr(e)
                print(f"Connection attempt {attempt} failed: {msg}")

    if last_exc:
        # Provide a helpful hint if driver was not found
        if any('IM002' in str(last_exc) or 'Data source name not found' in str(last_exc) for _ in (0,)):
            raise RuntimeError('ODBC driver not found. Install Microsoft ODBC Driver for SQL Server (17 or 18) and try again.') from last_exc
        raise last_exc
    raise RuntimeError('No connection-string candidates were attempted')


def ensure_silver_table(cursor):
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensor_logs_silver')
    BEGIN
        CREATE TABLE sensor_logs_silver (
            id INT IDENTITY(1,1) PRIMARY KEY,
            sensor_id NVARCHAR(255),
            value FLOAT NULL,
            unit NVARCHAR(50),
            ts DATETIME2 NULL,
            data_quality NVARCHAR(20),
            ingestion_time DATETIME2 DEFAULT SYSUTCDATETIME()
        );
    END
    ''')


def ensure_watermark_table(cursor):
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'silver_watermark')
    BEGIN
        CREATE TABLE silver_watermark (
            id INT IDENTITY(1,1) PRIMARY KEY,
            last_ts DATETIME2 NULL
        );
    END
    ''')


def get_transform_date(cursor):
    cursor.execute('SELECT TOP 1 last_ts FROM silver_watermark ORDER BY id DESC')
    r = cursor.fetchone()
    return r[0] if r and r[0] is not None else None


def set_transform_date(cursor, ts):
    cursor.execute('DELETE FROM silver_watermark')
    cursor.execute('INSERT INTO silver_watermark (last_ts) VALUES (?)', (ts,))


def normalize_unit(unit):
    if not unit:
        return None
    u = str(unit).lower()
    if 'thermostat' in u or 'celsius' in u or 'c' == u.strip().lower():
        return 'Celsius'
    if 'percent' in u or '%' in u or 'humid' in u:
        return 'Percentage'
    return unit


def parse_value(val):
    if val is None:
        return None, 'missing'
    try:
        # Accept numeric or stringified numeric
        v = float(val)
        return v, 'ok'
    except Exception:
        return None, 'bad_type'


def parse_timestamp(ts_str):
    if not ts_str:
        return None, 'missing'
    # try common format from producer: 'YYYY-MM-DD HH:MM:SS'
    try:
        return datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S'), 'ok'
    except Exception:
        # try ISO
        try:
            return datetime.datetime.fromisoformat(ts_str), 'ok'
        except Exception:
            return None, 'bad_ts'


def clean_and_store(poll_interval_seconds=1):
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        ensure_silver_table(cur)
        ensure_watermark_table(cur)

        # initialize transform_date from watermark or from existing silver data
        transform_date = get_transform_date(cur)
        if transform_date is None:
            cur.execute('SELECT MAX(ts) FROM sensor_logs_silver')
            r = cur.fetchone()
            transform_date = r[0] if r and r[0] is not None else None
        print(f'Starting stream with transform_date={transform_date}')

        while True:
            try:
                cur.execute('SELECT id, sensor_id, val, unit, ts FROM sensor_logs')
                rows = cur.fetchall()

                # Compute per-sensor averages from already stored silver values
                cur.execute('SELECT sensor_id, AVG(value) FROM sensor_logs_silver WHERE value IS NOT NULL GROUP BY sensor_id')
                avg_rows = cur.fetchall()
                silver_averages = {r[0]: r[1] for r in avg_rows}

                # Build enriched rows for processing: parse ts and values ahead
                to_process = []
                max_ts = transform_date
                for r in rows:
                    rid, sensor_id, val, unit, ts = r
                    parsed_ts, ts_status = parse_timestamp(ts)
                    if parsed_ts and (transform_date is None or parsed_ts > transform_date):
                        v, v_status = parse_value(val)
                        to_process.append({
                            'id': rid,
                            'sensor_id': sensor_id,
                            'raw_val': val,
                            'value': v,
                            'val_status': v_status,
                            'unit': unit,
                            'ts': ts,
                            'parsed_ts': parsed_ts,
                            'ts_status': ts_status,
                        })
                        if max_ts is None or parsed_ts > max_ts:
                            max_ts = parsed_ts

                if not to_process:
                    time.sleep(poll_interval_seconds)
                    continue

                # Group rows by sensor and sort by timestamp
                sensor_groups = {}
                for item in to_process:
                    sid = item['sensor_id'] or '__unknown__'
                    sensor_groups.setdefault(sid, []).append(item)
                for sid, items in sensor_groups.items():
                    items.sort(key=lambda x: x['parsed_ts'])

                inserted = 0
                imputed = 0
                processed_max_ts = None
                deferred_ts = []

                # For each sensor, attempt imputation using previous (silver) and next (batch) values
                for sid, items in sensor_groups.items():
                    # fetch latest silver value before the first item as previous candidate
                    prev_value = None
                    try:
                        first_ts = items[0]['parsed_ts']
                        cur.execute('SELECT TOP 1 value FROM sensor_logs_silver WHERE sensor_id = ? AND value IS NOT NULL AND ts < ? ORDER BY ts DESC', (sid, first_ts))
                        rprev = cur.fetchone()
                        if rprev and rprev[0] is not None:
                            prev_value = float(rprev[0])
                    except Exception:
                        prev_value = None

                    # iterate items
                    for idx, item in enumerate(items):
                        parsed_ts = item['parsed_ts']
                        val_status = item['val_status']
                        value = item['value']
                        norm_unit = normalize_unit(item['unit'])

                        # If value is present, insert as normal
                        if val_status == 'ok':
                            dq = 'clean' if item['ts_status'] == 'ok' and item['sensor_id'] else 'dirty'
                            cur.execute(
                                'INSERT INTO sensor_logs_silver (sensor_id, value, unit, ts, data_quality) VALUES (?, ?, ?, ?, ?)',
                                (item['sensor_id'], value, norm_unit, parsed_ts, dq)
                            )
                            inserted += 1
                            prev_value = value
                            processed_max_ts = parsed_ts if processed_max_ts is None or parsed_ts > processed_max_ts else processed_max_ts
                            continue

                        # For missing/bad value: find next valid value in the same batch
                        next_value = None
                        for future in items[idx+1:]:
                            if future['val_status'] == 'ok':
                                next_value = future['value']
                                break

                        # If both prev_value and next_value exist, impute as their average
                        if prev_value is not None and next_value is not None:
                            imputed_value = (prev_value + next_value) / 2.0
                            # only mark as imputed if timestamp parsed OK
                            if item['ts_status'] == 'ok' and item['sensor_id']:
                                dq = 'imputed'
                            else:
                                dq = 'dirty'
                            cur.execute(
                                'INSERT INTO sensor_logs_silver (sensor_id, value, unit, ts, data_quality) VALUES (?, ?, ?, ?, ?)',
                                (item['sensor_id'], imputed_value, norm_unit, parsed_ts, dq)
                            )
                            inserted += 1
                            imputed += 1
                            prev_value = imputed_value
                            processed_max_ts = parsed_ts if processed_max_ts is None or parsed_ts > processed_max_ts else processed_max_ts
                            continue

                        # If next_value is missing (this row is the last known in batch), skip and defer
                        # It will be re-evaluated in a future batch when a "next" value may exist.
                        print(f"Deferring imputation for sensor={sid} ts={parsed_ts} (no next value yet)")
                        # remember deferred timestamps so we don't advance global watermark past them
                        try:
                            if parsed_ts is not None:
                                deferred_ts.append(parsed_ts)
                        except Exception:
                            pass
                        # do not insert; do not advance watermark beyond this row

                # adjust watermark so we never advance beyond the earliest deferred timestamp
                if deferred_ts:
                    try:
                        min_deferred = min(deferred_ts)
                        if processed_max_ts is not None and processed_max_ts >= min_deferred:
                            # step back one microsecond so deferred row remains > watermark
                            processed_max_ts = min_deferred - datetime.timedelta(microseconds=1)
                    except Exception:
                        pass

                # update watermark and commit only if we processed at least one row beyond previous watermark
                if processed_max_ts is not None and (transform_date is None or processed_max_ts > transform_date):
                    set_transform_date(cur, processed_max_ts)
                    conn.commit()
                    transform_date = processed_max_ts
                    print(f'Processed and inserted {inserted} rows (imputed={imputed}), new transform_date={transform_date}')
                else:
                    # nothing processed: sleep and retry
                    conn.rollback()
                    print('No rows were processed in this iteration (all deferred); sleeping before retry')
                    time.sleep(poll_interval_seconds)

            except Exception:
                conn.rollback()
                print('Error during streaming iteration:')
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


if __name__ == '__main__':
    clean_and_store()
