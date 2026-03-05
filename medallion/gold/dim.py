import time
import traceback
import argparse
import datetime

try:
    from medallion.silver.silver import get_connection
except ModuleNotFoundError:
    import os
    import sys

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from medallion.silver.silver import get_connection


DB_PREFIX = '[data_warehouse].[dbo].'
GOLD_FQ = DB_PREFIX + 'gold_central'
WM_TABLE = DB_PREFIX + 'dim_gold_watermark'


def _ensure_dim_sensor(cursor):
    cursor.execute(
        "IF NOT EXISTS (SELECT * FROM [data_warehouse].sys.tables WHERE name = 'dim_sensor' AND schema_id = SCHEMA_ID('dbo')) "
        "CREATE TABLE [data_warehouse].[dbo].dim_sensor ("
        "  sensor_id NVARCHAR(255) NOT NULL PRIMARY KEY,"
        "  first_seen DATETIME2 NULL,"
        "  last_seen DATETIME2 NULL"
        ");"
    )



def _execute_with_retry(cursor, sql, params=None, attempts=5):
    backoff = 0.1
    for attempt in range(1, attempts + 1):
        try:
            if params is None:
                return cursor.execute(sql)
            return cursor.execute(sql, params)
        except Exception as e:
            msg = str(e).lower()
            if 'deadlock' in msg or '1205' in msg or '40001' in msg:
                if attempt == attempts:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue
            raise


def _ensure_global_watermark(cursor):
    cursor.execute(
        f"IF NOT EXISTS (SELECT * FROM [data_warehouse].sys.tables WHERE name = 'dim_gold_watermark' AND schema_id = SCHEMA_ID('dbo')) "
        f"CREATE TABLE {WM_TABLE} (id INT IDENTITY(1,1) PRIMARY KEY, last_ts DATETIME2 NULL);"
    )


def _get_global_watermark(cursor):
    cursor.execute(f'SELECT TOP 1 last_ts FROM {WM_TABLE} ORDER BY id DESC')
    r = cursor.fetchone()
    return r[0] if r and r[0] is not None else None


def _set_global_watermark(cursor, ts):
    cursor.execute(f'DELETE FROM {WM_TABLE}')
    cursor.execute(f'INSERT INTO {WM_TABLE} (last_ts) VALUES (?)', (ts,))


def _column_exists(cursor, table_name, column_name):
    r = cursor.execute(
        "SELECT 1 FROM [data_warehouse].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ? AND COLUMN_NAME = ?",
        (table_name, column_name),
    ).fetchone()
    return r is not None


def ensure_dim_table(cursor, metric):
    """Ensure an append-only (event-style) dim_<metric> table exists.

    If an older snapshot-style dim table exists (unique sensor_id + last_value/last_ts),
    it gets renamed to dim_<metric>_snapshot and a fresh append table is created.
    """
    metric = (metric or '').strip().lower()
    if not metric:
        raise ValueError('metric must be non-empty')

    dim_table = f'dim_{metric}'
    full_dim = DB_PREFIX + dim_table

    _ensure_dim_sensor(cursor)

    # Does the table exist?
    exists = cursor.execute(
        "SELECT 1 FROM [data_warehouse].sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (dim_table,),
    ).fetchone() is not None

    if exists:
        # If this looks like the old snapshot schema, rename it out of the way.
        has_last_value = _column_exists(cursor, dim_table, 'last_value')
        has_last_ts = _column_exists(cursor, dim_table, 'last_ts')
        has_ts = _column_exists(cursor, dim_table, 'ts')
        has_value = _column_exists(cursor, dim_table, 'value')

        if (has_last_value or has_last_ts) and not (has_ts and has_value):
            snapshot_name = f'{dim_table}_snapshot'
            # Only rename if snapshot table name isn't already taken.
            snap_exists = cursor.execute(
                "SELECT 1 FROM [data_warehouse].sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
                (snapshot_name,),
            ).fetchone() is not None
            if not snap_exists:
                cursor.execute(f"EXEC sp_rename 'data_warehouse.dbo.{dim_table}', '{snapshot_name}'")
                exists = False

    if not exists:
        cursor.execute(
            f"CREATE TABLE {full_dim} ("
            f"  id INT IDENTITY(1,1) PRIMARY KEY,"
            f"  sensor_id NVARCHAR(255) NOT NULL,"
            f"  ts DATETIME2 NOT NULL,"
            f"  value FLOAT NULL,"
            f"  unit NVARCHAR(50) NULL,"
            f"  status NVARCHAR(20) NULL,"
            f"  pipeline_latency FLOAT NULL"
            f");"
        )
        cursor.execute(f"CREATE INDEX IX_{dim_table}_ts ON {full_dim}(ts)")
        cursor.execute(f"CREATE INDEX IX_{dim_table}_sensor_ts ON {full_dim}(sensor_id, ts)")

        # FK dim_<metric>.sensor_id -> dim_sensor.sensor_id
        fk_name = f'FK_{dim_table}_dim_sensor'
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM [data_warehouse].sys.foreign_keys WHERE name = ?) "
            f"ALTER TABLE {full_dim} WITH CHECK ADD CONSTRAINT " + fk_name + " FOREIGN KEY (sensor_id) REFERENCES [data_warehouse].[dbo].dim_sensor(sensor_id);",
            (fk_name,),
        )

    return full_dim


def ensure_all_dims(cursor, since_ts=None):
    """Ensure dim tables exist for all metrics present in gold_central (optionally only new ones since since_ts)."""
    try:
        if since_ts is None:
            metrics = cursor.execute(f"SELECT DISTINCT metric FROM {GOLD_FQ} WHERE metric IS NOT NULL").fetchall()
        else:
            metrics = cursor.execute(f"SELECT DISTINCT metric FROM {GOLD_FQ} WHERE metric IS NOT NULL AND ts > ?", (since_ts,)).fetchall()
    except Exception:
        metrics = []

    for r in metrics:
        metric = (r[0] or '').strip().lower()
        if not metric:
            continue
        try:
            ensure_dim_table(cursor, metric)
        except Exception:
            pass


def append_dim_from_gold(cursor, metric, last_ts=None):
    """Append new rows for a metric from gold_central into dim_<metric>."""
    metric = (metric or '').strip().lower()
    if not metric:
        return 0

    full_dim = ensure_dim_table(cursor, metric)

    # ensure sensors exist in dim_sensor for this metric
    cursor.execute(
        "INSERT INTO [data_warehouse].[dbo].dim_sensor (sensor_id, first_seen, last_seen) "
        f"SELECT DISTINCT gc.sensor_id, SYSUTCDATETIME(), SYSUTCDATETIME() FROM {GOLD_FQ} gc "
        "WHERE gc.metric = ? AND gc.sensor_id IS NOT NULL AND LTRIM(RTRIM(gc.sensor_id)) <> '' "
        "AND NOT EXISTS (SELECT 1 FROM [data_warehouse].[dbo].dim_sensor s WHERE s.sensor_id = gc.sensor_id)",
        (metric,),
    )

    # Append new rows, avoid obvious duplicates by (sensor_id, ts)
    if last_ts is None:
        cursor.execute(
            f"INSERT INTO {full_dim} (sensor_id, ts, value, unit, status, pipeline_latency) "
            f"SELECT gc.sensor_id, gc.ts, gc.value, gc.unit, gc.status, CAST(DATEDIFF(SECOND, gc.ts, SYSUTCDATETIME()) AS FLOAT) "
            f"FROM {GOLD_FQ} gc "
            f"WHERE gc.metric = ? "
            f"AND NOT EXISTS (SELECT 1 FROM {full_dim} d WHERE d.sensor_id = gc.sensor_id AND d.ts = gc.ts)",
            (metric,),
        )
    else:
        cursor.execute(
            f"INSERT INTO {full_dim} (sensor_id, ts, value, unit, status, pipeline_latency) "
            f"SELECT gc.sensor_id, gc.ts, gc.value, gc.unit, gc.status, CAST(DATEDIFF(SECOND, gc.ts, SYSUTCDATETIME()) AS FLOAT) "
            f"FROM {GOLD_FQ} gc "
            f"WHERE gc.metric = ? AND gc.ts > ? "
            f"AND NOT EXISTS (SELECT 1 FROM {full_dim} d WHERE d.sensor_id = gc.sensor_id AND d.ts = gc.ts)",
            (metric, last_ts),
        )

    try:
        return cursor.rowcount
    except Exception:
        return 0


def _parse_ts(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime.datetime):
        return ts
    if isinstance(ts, str):
        return datetime.datetime.fromisoformat(ts)
    return ts


def stream_dim_from_gold(poll_interval_seconds=1, run_once=False):
    conn = get_connection()
    cur = conn.cursor()
    try:
        _ensure_global_watermark(cur)
        conn.commit()

        last_ts = _get_global_watermark(cur)
        print('Starting dim processor from gold_central, last_ts=', last_ts)

        # make sure all existing metrics have dim tables (append style)
        ensure_all_dims(cur)
        conn.commit()

        while True:
            try:
                # ensure any newly appearing metrics also have tables
                ensure_all_dims(cur, since_ts=last_ts)

                # Append new rows per metric using set-based INSERTs
                if last_ts is None:
                    metric_rows = cur.execute(f"SELECT DISTINCT metric FROM {GOLD_FQ} WHERE metric IS NOT NULL").fetchall()
                else:
                    metric_rows = cur.execute(f"SELECT DISTINCT metric FROM {GOLD_FQ} WHERE metric IS NOT NULL AND ts > ?", (last_ts,)).fetchall()

                total_appended = 0
                for mr in metric_rows:
                    metric = (mr[0] or '').strip().lower()
                    if not metric:
                        continue
                    total_appended += append_dim_from_gold(cur, metric, last_ts=last_ts)

                # Compute next watermark
                if last_ts is None:
                    r = cur.execute(f"SELECT MAX(ts) FROM {GOLD_FQ}").fetchone()
                else:
                    r = cur.execute(f"SELECT MAX(ts) FROM {GOLD_FQ} WHERE ts > ?", (last_ts,)).fetchone()
                max_ts = r[0] if r and r[0] is not None else last_ts

                if total_appended == 0:
                    if run_once:
                        break
                    time.sleep(poll_interval_seconds)
                    continue
                if max_ts is not None:
                    _execute_with_retry(cur, f'DELETE FROM {WM_TABLE}')
                    _execute_with_retry(cur, f'INSERT INTO {WM_TABLE} (last_ts) VALUES (?)', (max_ts,))
                    last_ts = max_ts

                conn.commit()
                print(f'Appended {total_appended} rows; last_ts={last_ts}')

                if run_once:
                    break

            except Exception:
                conn.rollback()
                print('Error in dim processing iteration:')
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
    p = argparse.ArgumentParser(description='Create/seed dim_<metric> tables from gold_central and keep them updated using a watermark')
    p.add_argument('--interval', type=int, default=1, help='poll interval seconds')
    p.add_argument('--once', action='store_true', help='run once then exit')
    args = p.parse_args()

    try:
        stream_dim_from_gold(poll_interval_seconds=args.interval, run_once=args.once)
    except KeyboardInterrupt:
        print('Interrupted; shutting down dim processor')


if __name__ == '__main__':
    main()
