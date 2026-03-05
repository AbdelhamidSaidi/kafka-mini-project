# Kafka Mini Project — Medallion pipeline (local dev)

This repository contains a minimal end-to-end medallion-style pipeline for IoT sensor data: a Python producer → Kafka broker → consumer (bronze) → silver transforms → gold transforms → dim tables. The repo includes orchestration (Airflow DAG), a Streamlit dashboard for KPIs, and helper scripts for local development.

Key points:
- Kafka is used as the message bus (single-node broker in KRaft mode for local use).
- SQL Server (T-SQL) is used as the persistence layer (accessed via ODBC / `pyodbc`).
- The medallion layers are implemented under the `medallion/` package (`silver/`, `gold/`).
- A Streamlit dashboard in `app/streamlit_app.py` visualizes KPIs, per-sensor series, watermarks and latency.

Repository layout (important files)

- `producer.py` — synthetic producer that publishes sensor messages to Kafka (`sensor-data` topic).
- `consumer.py` — Kafka consumer that writes raw messages to `sensor_logs` (bronze).
- `medallion/` — ETL package with:
  - `medallion/silver/silver.py` — silver-layer logic and DB connection helpers
  - `medallion/gold/transform_gold.py` — gold transforms (builds `gold_central` and per-metric tables)
  - `medallion/gold/dim.py` — dim pipeline (append-style `dim_<metric>` tables seeded from `gold_central`)
  - `medallion/gold/gold.py` — runner/orchestrator for gold+dim loops
- `app/streamlit_app.py` — dashboard with auto-refresh, per-sensor charts, last-minute/last-second variation, and KPIs
- `dags/orchestrate_pipeline.py` — Airflow DAG to orchestrate: producer → consumer → silver → gold (with data tests between stages)
- `docker-compose.yaml` — brings up local services (Kafka broker, Airflow service, etc.)
- `docker/airflow/Dockerfile` — custom Airflow image (adds ODBC driver / Python provider packages)
- `docker/kafka/Dockerfile` — Kafka image wrapper (if present)
- `scripts/` — helper scripts:
  - `generate_mssql_env.py` — builds `.env` with `MSSQL_ALCHEMY_CONN` from `medallion/silver/silver.py` config
  - `data_tests.py` — run boundary/data-quality tests between layers
  - `diagnose_dim.py` — diagnostic utility for `dim` tables
- `requirements.txt` — Python dependencies (Streamlit, pandas, pyodbc, kafka-python, plotly, streamlit-autorefresh)

Prerequisites

- Python 3.9+ (use a virtual environment for local runs)
- Docker & Docker Compose (for the Kafka + Airflow environment)
- Access to a SQL Server instance reachable from your runtime (or configure a Dockerized SQL Server with appropriate licensing)
- On Windows, install Microsoft ODBC Driver for SQL Server (e.g. ODBC Driver 18). Containers use `msodbcsql` installed in the Airflow image.

Quick local development (without Docker)

1. Create and activate a venv, install deps:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Start the producer and consumer in separate terminals (consumer needs a valid SQL Server connection string):

```powershell
python producer.py
python consumer.py
```

3. Run the gold + dim pipelines locally (from repo root):

```powershell
python medallion/gold/gold.py        # runs gold+dim loop
# or run a single iteration / only gold / only dim
python medallion/gold/gold.py --once
python medallion/gold/gold.py --gold-only
python medallion/gold/gold.py --dim-only
```

4. Open the dashboard:

```powershell
streamlit run app/streamlit_app.py
```

Docker / Airflow quick start

1. Generate `.env` with SQLAlchemy connection (reads `medallion/silver/silver.py` configuration):

```powershell
# Copy the example env and set a strong SA password before starting
copy .env.example .env
# Edit `.env` and set `MSSQL_SA_PASSWORD` (and other vars if needed)
python scripts/generate_mssql_env.py
```

2. Build and start services (may take several minutes while images build):

```powershell
docker compose up --build -d
```

Notes:
- The Airflow image installs the Microsoft ODBC driver; building that image requires network access and apt permissions during the Docker build. If the build fails due to missing packages or network issues, inspect the build logs and try again on a machine with internet access.
- `MSSQL_ALCHEMY_CONN` is read from `.env` by the Airflow container to configure the metadata DB connection.

Data tests

Run repository data-quality checks between layers using:

```powershell
python scripts/data_tests.py --boundary silver_gold
```

Streamlit dashboard features

- Auto-refreshing KPI dashboard (5s) showing:
  - Table counts and watermark timestamps
  - Per-sensor last-minute time series (one line per sensor)
  - Per-sensor total average and last-minute average
  - Last-second variation charts and recent-series panels
  - Pipeline latency (per-dim table) and last-element latency per table

Troubleshooting

- ODBC/pyodbc errors: ensure the Microsoft ODBC driver is installed and visible to `pyodbc`.
- Docker image build failures for Airflow: check the Docker build logs for missing apt packages or network errors; building the image on a machine with internet access usually resolves the msodbcsql install steps.
- Streamlit shows an import error for `medallion`: run `pip install -r requirements.txt` and run Streamlit from the repo root (`streamlit run app/streamlit_app.py`).

Contributing and next steps

- Add integration tests that exercise the full pipeline with a test Kafka broker and ephemeral SQL Server instance.
- Consider adding unique constraints and per-sensor watermarks for scalability in production.


