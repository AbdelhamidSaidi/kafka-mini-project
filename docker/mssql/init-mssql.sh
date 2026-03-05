#!/bin/bash
set -e

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

echo "Waiting for SQL Server to start..."
for i in {1..60}; do
  /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$SA_PASSWORD" -Q "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done

# Run initialization scripts
if [ -d "/initdb" ]; then
  for f in /initdb/*.sql; do
    if [ -f "$f" ]; then
      echo "Running $f"
      /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$SA_PASSWORD" -i "$f"
    fi
  done
fi

# Wait on sqlservr to keep the container alive
wait
