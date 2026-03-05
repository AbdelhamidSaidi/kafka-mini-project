import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import traceback

# optional autorefresh helper
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

# Ensure project root is on sys.path so `medallion` imports work when Streamlit
# runs from the `app/` directory.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from medallion.silver.silver import get_connection


st.set_page_config(page_title='IoT Sensors Dashboard', layout='wide')

st.title('IoT Sensors Dashboard')


@st.cache_data(ttl=15)
def fetch_metrics(reload_counter=None):
    try:
        conn = get_connection()
    except Exception as e:
        return {'error': str(e)}

    try:
        cur = conn.cursor()

        # discover dim tables
        cur.execute("SELECT name FROM sys.tables WHERE name LIKE 'dim_%' AND name NOT LIKE '%watermark%'")
        dim_tables = [r[0] for r in cur.fetchall()]

        base_tables = ['sensor_logs', 'sensor_logs_silver', 'gold_central', 'dim_sensor']
        tables = base_tables + dim_tables

        counts = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = int(cur.fetchone()[0])
            except Exception:
                counts[t] = None

        # status distribution from gold_central
        status_df = pd.DataFrame()
        try:
            cur.execute('SELECT status, COUNT(*) FROM gold_central GROUP BY status')
            rows = cur.fetchall()
            status_df = pd.DataFrame(rows, columns=['status', 'count'])
        except Exception:
            status_df = pd.DataFrame()

        # pipeline latency across dim tables — take value from row with biggest id per table
        latency_rows = []
        for dt in dim_tables:
            try:
                cur.execute(f"SELECT TOP 1 pipeline_latency FROM {dt} WHERE pipeline_latency IS NOT NULL ORDER BY id DESC")
                r = cur.fetchone()
                if r and r[0] is not None:
                    latency_rows.append({'table': dt, 'latency': float(r[0])})
            except Exception:
                continue
        df_latency = pd.DataFrame(latency_rows)

        # watermarks
        wm = {}
        for w in ('silver_watermark', 'gold_watermark', 'dim_gold_watermark'):
            try:
                cur.execute(f"SELECT TOP 1 last_ts FROM {w} ORDER BY id DESC")
                r = cur.fetchone()
                wm[w] = r[0] if r and r[0] is not None else None
            except Exception:
                wm[w] = None

        # latency of last element per table (seconds since last timestamp)
        latencies = {}
        for t in tables:
            try:
                last_ts = None
                for col in ('ts', 'ingestion_time', 'last_seen', 'first_seen'):
                    # check column exists
                    try:
                        cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?", (t, col))
                        if cur.fetchone():
                            cur.execute(f"SELECT MAX([{col}]) FROM {t}")
                            r = cur.fetchone()
                            if r and r[0] is not None:
                                last_ts = r[0]
                                break
                    except Exception:
                        continue

                if last_ts is None:
                    latencies[t] = {'last_ts': None, 'latency_s': None}
                else:
                    # compute latency in seconds (use UTC now)
                    try:
                        if isinstance(last_ts, str):
                            last_dt = pd.to_datetime(last_ts)
                        else:
                            last_dt = last_ts
                        # normalize to naive UTC for subtraction
                        if hasattr(last_dt, 'tzinfo') and last_dt.tzinfo is not None:
                            last_dt = last_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                        now = datetime.datetime.utcnow()
                        latency_sec = (now - last_dt).total_seconds()
                        latencies[t] = {'last_ts': last_dt, 'latency_s': float(latency_sec)}
                    except Exception:
                        latencies[t] = {'last_ts': last_ts, 'latency_s': None}
            except Exception:
                latencies[t] = {'last_ts': None, 'latency_s': None}

        # last-minute per-sensor recent metrics (last 1 minute)
        recent = {}
        try:
            cur.execute("SELECT DISTINCT sensor_id, metric FROM gold_central WHERE ts >= DATEADD(MINUTE, -1, SYSUTCDATETIME())")
            sensor_rows = cur.fetchall()
            for r in sensor_rows:
                sid = r[0]
                metric = r[1] if len(r) > 1 else None

                # time series for last minute
                cur.execute(
                    "SELECT ts, value, status FROM gold_central WHERE sensor_id = ? AND ts >= DATEADD(MINUTE, -1, SYSUTCDATETIME()) ORDER BY ts",
                    (sid,)
                )
                srows = cur.fetchall()
                if srows:
                    df_series = pd.DataFrame(srows, columns=['ts', 'value', 'status'])
                    try:
                        df_series['ts'] = pd.to_datetime(df_series['ts'])
                    except Exception:
                        pass
                    last_status = df_series['status'].iloc[-1] if not df_series.empty else None
                    last_ts_val = df_series['ts'].iloc[-1] if not df_series.empty and 'ts' in df_series.columns else None
                else:
                    df_series = pd.DataFrame(columns=['ts', 'value', 'status'])
                    last_status = None
                    last_ts_val = None

                # data quality counts from silver for last minute
                cur.execute(
                    "SELECT data_quality, COUNT(*) FROM sensor_logs_silver WHERE sensor_id = ? AND ts >= DATEADD(MINUTE, -1, SYSUTCDATETIME()) GROUP BY data_quality",
                    (sid,)
                )
                qrows = cur.fetchall()
                dq = {qr[0]: int(qr[1]) for qr in qrows} if qrows else {}

                # compute overall average value for this sensor (across gold_central)
                total_avg = None
                try:
                    cur.execute("SELECT AVG(value) FROM gold_central WHERE sensor_id = ? AND value IS NOT NULL", (sid,))
                    r_avg = cur.fetchone()
                    if r_avg and r_avg[0] is not None:
                        total_avg = float(r_avg[0])
                except Exception:
                    total_avg = None

                # compute last-minute average from df_series
                last_minute_avg = None
                try:
                    if df_series is not None and not df_series.empty and 'value' in df_series.columns:
                        last_minute_avg = float(pd.to_numeric(df_series['value'], errors='coerce').mean())
                except Exception:
                    last_minute_avg = None

                recent[sid] = {
                    'metric': metric,
                    'series': df_series,
                    'last_status': last_status,
                    'data_quality': dq,
                    'total_avg': total_avg,
                    'last_minute_avg': last_minute_avg,
                    'last_ts': last_ts_val,
                }
        except Exception:
            recent = {}

        # per-metric variation and recent series for last minute
        metric_variation = pd.DataFrame()
        metric_series = {}
        metric_variation_1s = {}
        metric_series_1s = {}
        try:
            cur.execute("SELECT metric, sensor_id, MIN(value) AS min_v, MAX(value) AS max_v FROM gold_central WHERE ts >= DATEADD(MINUTE, -1, SYSUTCDATETIME()) GROUP BY metric, sensor_id")
            mv_rows = cur.fetchall()
            mv_list = []
            for r in mv_rows:
                metric = r[0]
                sid = r[1]
                min_v = r[2]
                max_v = r[3]
                try:
                    variation = None if min_v is None or max_v is None else float(max_v - min_v)
                except Exception:
                    variation = None
                mv_list.append((metric, sid, min_v, max_v, variation))
            if mv_list:
                metric_variation = pd.DataFrame(mv_list, columns=['metric', 'sensor_id', 'min_v', 'max_v', 'variation'])

            # also fetch per-metric time series for the last minute (small windows)
            metrics = metric_variation['metric'].dropna().unique().tolist() if not metric_variation.empty else []
            for m in metrics:
                try:
                    cur.execute('SELECT ts, sensor_id, value FROM gold_central WHERE metric = ? AND ts >= DATEADD(MINUTE, -1, SYSUTCDATETIME()) ORDER BY ts', (m,))
                    rows = cur.fetchall()
                    if rows:
                        dfm = pd.DataFrame(rows, columns=['ts', 'sensor_id', 'value'])
                        try:
                            dfm['ts'] = pd.to_datetime(dfm['ts'])
                        except Exception:
                            pass
                        metric_series[m] = dfm
                except Exception:
                    continue
            # per-metric variation and series for the last second
            try:
                cur.execute("SELECT metric, ts, sensor_id, value FROM gold_central WHERE ts >= DATEADD(SECOND, -1, SYSUTCDATETIME()) ORDER BY metric, ts")
                rows = cur.fetchall()
                if rows:
                    df_all = pd.DataFrame(rows, columns=['metric', 'ts', 'sensor_id', 'value'])
                    try:
                        df_all['ts'] = pd.to_datetime(df_all['ts'])
                    except Exception:
                        pass
                    for m, g in df_all.groupby('metric'):
                        g_sorted = g.sort_values('ts')
                        metric_series_1s[m] = g_sorted[['ts', 'sensor_id', 'value']].reset_index(drop=True)
                        var_df = g_sorted.groupby('ts')['value'].agg(['min', 'max']).reset_index()
                        var_df['variation'] = var_df['max'] - var_df['min']
                        metric_variation_1s[m] = var_df[['ts', 'variation']]
            except Exception:
                metric_variation_1s = {}
                metric_series_1s = {}
        except Exception:
            metric_variation = pd.DataFrame()
            metric_series = {}

        return {
            'counts': counts,
            'status_df': status_df,
            'df_latency': df_latency,
            'watermarks': wm,
            'recent': recent,
            'latencies': latencies,
            'metric_variation': metric_variation,
            'metric_series': metric_series,
            'metric_variation_1s': metric_variation_1s,
            'metric_series_1s': metric_series_1s,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    st.sidebar.header('Controls')
    # Auto-refresh graphs every 5 seconds (uses streamlit-autorefresh if installed)
    try:
        if st_autorefresh is not None:
            refresh_count = st_autorefresh(interval=5000, key='autorefresh')
        else:
            if 'manual_refresh' not in st.session_state:
                st.session_state.manual_refresh = 0
            if st.sidebar.button('Refresh'):
                st.session_state.manual_refresh += 1
            refresh_count = st.session_state.manual_refresh
    except Exception:
        if 'manual_refresh' not in st.session_state:
            st.session_state.manual_refresh = 0
        if st.sidebar.button('Refresh'):
            st.session_state.manual_refresh += 1
        refresh_count = st.session_state.manual_refresh

    with st.spinner('Fetching metrics...'):
        data = fetch_metrics(reload_counter=refresh_count)

    if 'error' in data:
        st.error('DB connection failed: ' + data['error'])
        st.write(traceback.format_exc())
        return

    counts = data['counts']
    status_df = data['status_df']
    df_latency = data['df_latency']
    wm = data['watermarks']

    # Overview counts
    st.subheader('Table Counts')
    df_counts = pd.DataFrame(list(counts.items()), columns=['table', 'count'])
    st.dataframe(df_counts)
    try:
        st.plotly_chart(px.bar(df_counts, x='table', y='count', title='Table row counts'))
    except Exception:
        pass

    # Status distribution
    st.subheader('Gold status distribution')
    if not status_df.empty:
        st.dataframe(status_df)
        fig = px.pie(status_df, names='status', values='count', title='gold_central status')
        st.plotly_chart(fig)
    else:
        st.info('gold_central not available or contains no rows yet')

    # Latency
    st.subheader('Pipeline latency (dim tables)')
    if not df_latency.empty:
        st.dataframe(df_latency.head(200))
        fig = px.box(df_latency, x='table', y='latency', title='Pipeline latency by dim table')
        st.plotly_chart(fig)
        hist = px.histogram(df_latency, x='latency', nbins=50, title='Latency distribution')
        st.plotly_chart(hist)
    else:
        st.info('No pipeline latency metrics available yet')

    # Watermarks
    st.subheader('Watermarks')
    wm_df = pd.DataFrame(list(wm.items()), columns=['watermark_table', 'last_ts'])
    st.table(wm_df)

    # Last-element latency per table
    st.subheader('Last-element latency (seconds)')
    latencies = data.get('latencies', {}) or {}
    if not latencies:
        st.info('No latency information available')
    else:
        lat_rows = []
        for t, v in latencies.items():
            last_ts = v.get('last_ts')
            ls = v.get('latency_s')
            lat_rows.append((t, str(last_ts) if last_ts is not None else None, None if ls is None else round(ls, 2)))
        df_lat = pd.DataFrame(lat_rows, columns=['table', 'last_ts', 'latency_s'])
        st.dataframe(df_lat.sort_values(by=['latency_s'], ascending=False).reset_index(drop=True))
        try:
            st.plotly_chart(px.bar(df_lat, x='table', y='latency_s', title='Latency (s) by table'), use_container_width=True)
        except Exception:
            pass

    # Last-minute sensor visuals (variation + status + data quality)
    st.subheader('Last-minute sensor metrics')
    recent = data.get('recent', {}) or {}
    if not recent:
        st.info('No recent data in the last minute')
    else:
        # For each sensor show a time-series and a small audit panel
        sensors = sorted(recent.keys())
        for sid in sensors:
            info = recent[sid]
            metric_name = info.get('metric') or ''
            df = info.get('series')
            last_status = info.get('last_status')
            dq = info.get('data_quality') or {}

            st.markdown(f"**Sensor: {sid}**  —  {metric_name}")
            left, right = st.columns([3, 1])
            with left:
                if df is None or df.empty:
                    st.info('No samples for the last minute')
                else:
                    try:
                        fig = px.line(df, x='ts', y='value', title=f'{sid} — last minute values')
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception:
                        st.line_chart(df.set_index('ts')['value'])

            with right:
                # status
                st.markdown('**Last status**')
                st.metric(label='', value=last_status or 'N/A')

                # averages
                total_avg = info.get('total_avg')
                last_minute_avg = info.get('last_minute_avg')
                last_ts_val = info.get('last_ts')
                st.markdown('**Averages**')
                try:
                    st.metric('Total avg', f'{total_avg:.3f}' if total_avg is not None else 'N/A')
                except Exception:
                    st.write('Total avg: ' + (str(total_avg) if total_avg is not None else 'N/A'))
                try:
                    st.metric('Last-minute avg', f'{last_minute_avg:.3f}' if last_minute_avg is not None else 'N/A')
                except Exception:
                    st.write('Last-minute avg: ' + (str(last_minute_avg) if last_minute_avg is not None else 'N/A'))

                # most recent sample timestamp
                st.markdown('**Most recent sample**')
                if last_ts_val is not None:
                    try:
                        st.write(str(last_ts_val))
                    except Exception:
                        st.write(last_ts_val)
                else:
                    st.write('N/A')

                # variation metric
                if df is not None and not df.empty:
                    try:
                        vmin = df['value'].min()
                        vmax = df['value'].max()
                        variation = vmax - vmin
                        st.metric('Variation (max-min)', f'{variation:.3f}')
                    except Exception:
                        pass

                # data quality audit
                st.markdown('**Data quality (last minute)**')
                if dq:
                    dq_df = pd.DataFrame(list(dq.items()), columns=['quality', 'count'])
                    try:
                        fig2 = px.pie(dq_df, names='quality', values='count', title='Data quality')
                        st.plotly_chart(fig2, use_container_width=True)
                    except Exception:
                        st.table(dq_df)
                else:
                    st.info('No silver rows in last minute')

    # Metric variation across sensors (last minute)
    st.subheader('Metric variation (last minute)')
    metric_variation = data.get('metric_variation')
    metric_series = data.get('metric_series') or {}
    if metric_variation is None or metric_variation.empty:
        st.info('No metric variation data available for last minute')
    else:
        try:
            fig_var = px.bar(metric_variation, x='sensor_id', y='variation', color='metric', barmode='group', title='Variation (max-min) per sensor by metric (last minute)')
            st.plotly_chart(fig_var, use_container_width=True)
        except Exception:
            st.dataframe(metric_variation)

    # Per-metric series for the last minute (one chart per metric)
    if metric_series:
        for metric, dfm in metric_series.items():
            st.markdown(f"**Metric: {metric} — last minute series by sensor**")
            if dfm is None or dfm.empty:
                st.info('No samples for this metric in the last minute')
                continue
            try:
                figm = px.line(dfm, x='ts', y='value', color='sensor_id', title=f'{metric} — last minute series')
                st.plotly_chart(figm, use_container_width=True)
            except Exception:
                try:
                    st.line_chart(dfm.pivot(index='ts', columns='sensor_id', values='value'))
                except Exception:
                    st.dataframe(dfm)

    # Metric variation and series for the last second
    st.subheader('Metric variation (last second)')
    metric_variation_1s = data.get('metric_variation_1s') or {}
    metric_series_1s = data.get('metric_series_1s') or {}
    if not metric_variation_1s:
        st.info('No metric data in the last second')
    else:
        for metric, var_df in metric_variation_1s.items():
            st.markdown(f"**Metric: {metric} — variation over last second**")
            if var_df is None or var_df.empty:
                st.info('No variation samples for this metric in the last second')
            else:
                try:
                    figv = px.line(var_df, x='ts', y='variation', title=f'{metric} variation (last second)')
                    st.plotly_chart(figv, use_container_width=True)
                except Exception:
                    st.dataframe(var_df)

            # plot raw series if present
            dfm1 = metric_series_1s.get(metric)
            if dfm1 is not None and not dfm1.empty:
                try:
                    figm1 = px.line(dfm1, x='ts', y='value', color='sensor_id', title=f'{metric} — last second series by sensor')
                    st.plotly_chart(figm1, use_container_width=True)
                except Exception:
                    try:
                        st.line_chart(dfm1.pivot(index='ts', columns='sensor_id', values='value'))
                    except Exception:
                        st.dataframe(dfm1)

    # Quick actions
    st.sidebar.subheader('Actions')
    if st.sidebar.button('Run data tests'):
        st.sidebar.info('Running data tests...')
        import subprocess
        try:
            res = subprocess.run(['python', 'scripts/data_tests.py', '--boundary', 'consumer_silver'], capture_output=True, text=True, timeout=120)
            st.sidebar.write(res.stdout)
            if res.returncode != 0:
                st.sidebar.error('Tests failed')
        except Exception as e:
            st.sidebar.error(str(e))


if __name__ == '__main__':
    main()
