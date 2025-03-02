import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Define the dbt profile configuration for Snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",  # Airflow connection ID for Snowflake
        profile_args={"database": "dataexpert_student", "schema": "lokesh_k"},  # Database and schema in Snowflake
    )
)

# Define the DAG for running dbt tasks
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dbt_project/dags"),  # Path to dbt project directory
    operator_args={"install_deps": True},  # Install dependencies if needed
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
    ),
    schedule_interval="* * * * *",  # Schedule DAG to run daily
    start_date=datetime(2025, 2, 23),  # Start date for DAG runs
    catchup=False,  # Disable backfilling of missed runs
    max_active_runs=1, 
    dag_id="rail_and_weather_dag",  # Unique ID for the DAG
)
