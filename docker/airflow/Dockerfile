FROM apache/airflow:2.8.1

USER root

# Install prerequisites and Microsoft ODBC driver for SQL Server
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl gnupg apt-transport-https ca-certificates build-essential unixodbc-dev wget \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages as the non-root `airflow` user to satisfy the image policy
# Skip the `apache-airflow-providers-microsoft-mssql` provider to avoid building
# `pymssql` from source inside the image (can be added later with proper build deps).
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir pyodbc
