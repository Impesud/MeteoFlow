from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="meteo_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meteo"],
) as dag:

    # 1️⃣ Estrazione raw JSONL → hourly CSV
    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/src/scripts/extract.py",
    )

    # 2️⃣ Caricamento dimensioni (regions, provinces, cities)
    load_dimensions = BashOperator(
        task_id="load_dimensions",
        bash_command="python /opt/airflow/src/scripts/load_dimensions.py",
    )

    # 3️⃣ Popola geom e region_istat in cities
    populate_region = BashOperator(
        task_id="populate_region_in_cities",
        bash_command=(
            'PGPASSWORD="$POSTGRES_PASSWORD" '
            "psql "
            '-h "$POSTGRES_HOST" '
            '-p "$POSTGRES_PORT" '
            '-U "$POSTGRES_USER" '
            '-d "$POSTGRES_DB" '
            "-f /opt/airflow/dags/sql/post_load.sql"
        ),
    )

    # 4️⃣ Bulk‑load in weather_city_hourly
    load_facts = BashOperator(
        task_id="load_facts",
        bash_command="python /opt/airflow/src/scripts/bulk_copy.py",
    )

    # 5️⃣ Aggregazioni separate per scope in parallelo
    with TaskGroup("aggregate", tooltip="Aggregations per scope") as aggregate_group:
        agg_city = BashOperator(
            task_id="agg_city",
            bash_command="python /opt/airflow/src/scripts/aggregate_sql.py --scope city",
        )
        agg_prov = BashOperator(
            task_id="agg_prov",
            bash_command="python /opt/airflow/src/scripts/aggregate_sql.py --scope prov",
        )
        agg_reg = BashOperator(
            task_id="agg_reg",
            bash_command="python /opt/airflow/src/scripts/aggregate_sql.py --scope reg",
        )

    # ───────────────────────────────────────────────
    # Orchestrazione finale
    # ───────────────────────────────────────────────
    extract >> load_dimensions >> populate_region >> load_facts >> aggregate_group
