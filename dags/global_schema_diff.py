import os
import json

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule
from pendulum.tz.timezone import Timezone

_BASE_PATH = "/usr/local/airflow/dags"
def _get_schema_diff_config(**kwargs):
    """
    A method for getting schema diff config
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", "databricks", "schema_diff_config.json")
    with open(file_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    ti.xcom_push(key="job_id", value=config["job_id"])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('global_schema_diff',
    start_date=datetime(2023, 1, 4, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["global", "validation", "daily"]
    ) as dag:

    database = "auto_report"
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_schema_diff_config = PythonOperator(
        task_id="get_schema_diff_config",
        python_callable=_get_schema_diff_config,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    schema_diff = DatabricksRunNowOperator(
        task_id="schema_diff",
        job_id="{{ ti.xcom_pull(task_ids='get_schema_diff_config', key='job_id') }}",
        notebook_params={
            "database": database
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start >> get_schema_diff_config >> schema_diff >> end