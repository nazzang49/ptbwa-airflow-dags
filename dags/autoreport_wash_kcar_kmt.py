from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import timedelta

import pendulum
import json

notebook_params = dict()

file_path = "/usr/local/airflow/dags/kcar_kmt.json"
with open(file_path, "r", encoding="utf-8") as f:
    notebook_params["washing_params"] = json.dumps(json.load(f))
    notebook_params["next_execution_date"] = '{{ next_execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S") }}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=3)
}

with DAG('autoreport_wash_kcar_kmt',
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    # end_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    schedule_interval='0 2 5 * *',
    catchup=False,
    default_args=default_args,
    # render_template_as_native_obj=True
    ) as dag:

    washing_run = DatabricksRunNowOperator(
        task_id="washing_task",
        job_id="751730826324009",
        databricks_conn_id='databricks_default',
        notebook_params=notebook_params
    )

    start_run = DummyOperator(task_id="start")
    end_run = DummyOperator(task_id="end")

    start_run >> washing_run >> end_run