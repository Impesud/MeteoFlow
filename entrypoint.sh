#!/bin/bash
set -euo pipefail

# Carica le variabili da .env
if [ -f "/opt/airflow/.env" ]; then
  export $(grep -v '^#' /opt/airflow/.env | xargs)
fi

echo "⏳ Attendo che Postgres sia pronto..."

PGHOST=${POSTGRES_HOST:-postgres}
PGPORT=${POSTGRES_PORT:-5432}
PGUSER=${POSTGRES_USER}
PGPASSWORD=${POSTGRES_PASSWORD}
PGDATABASE=${POSTGRES_DB}

export PGPASSWORD

while ! pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; do
  sleep 1
done

echo "✅ Postgres (${PGDATABASE}) pronto."

echo "✅ Inizializzo il DB di Airflow..."
airflow db migrate

echo "👤 Creo utente Admin (se non esiste)..."
airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-meteoflow}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-meteoflow}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Erik}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-Jara}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-erick.jara@hotmail.it}" \
  || echo "⚠️ Utente già esistente, skip"

echo "🧹 Rimuovo eventuale PID residuo..."
rm -f /opt/airflow/airflow-webserver.pid

echo "🚀 Avvio scheduler e webserver..."
airflow scheduler & exec airflow webserver




