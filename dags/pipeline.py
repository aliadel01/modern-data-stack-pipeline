from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from bronze_scripts.load_bronze import load_bronze

PATH_TO_DBT_PROJECT = "/usr/local/airflow/dbt/dbt_project"
PATH_TO_DBT_VENV = "/usr/local/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
    ),
)


execution_config = ExecutionConfig(
    dbt_executable_path=PATH_TO_DBT_VENV,  # dbt installed inside container
)
with DAG(
    dag_id="pipeline",
    max_active_runs=1,
    max_active_tasks=10,
    catchup=False,
    start_date=datetime(2024, 1, 1),
) as dag:

    load_bronze_task = PythonOperator(
        task_id="load_bronze_task",
        python_callable=load_bronze,
    )

    transform_data = DbtTaskGroup(
        group_id="run_dbt",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    load_bronze_task >> transform_data