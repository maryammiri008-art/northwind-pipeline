import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = Path(os.getenv("PROJECT_HOME", str(Path.home() / "de_final_project")))
PYTHON = PROJECT_HOME / "venv" / "bin" / "python"
LOAD_SCRIPT = PROJECT_HOME / "python" / "load_clickhouse.py"

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="northwind_incremental_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/1 * * * *", 
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["northwind", "final-project", "incremental"],
) as dag:
    

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=f"{PYTHON} {LOAD_SCRIPT}",
        env={**os.environ, "PROJECT_HOME": str(PROJECT_HOME)},
    )
    
    load_clickhouse
