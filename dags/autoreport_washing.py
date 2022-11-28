from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

import os
import json

notebook_params = dict()
notebook_names = list()
base_path = "/usr/local/airflow/dags"  # based on MWAA
for config_file in os.listdir(base_path):
    config = config_file.split(".")

    if len(config) <= 1:
        continue

    if config[1] == "json":
        notebook_names.append(config[0])
        with open(os.path.join(base_path, config_file), "r", encoding="utf-8") as f:
            notebook_params[config[0]] = json.dumps(json.load(f))

notebook_params["notebook_names"] = ",".join(notebook_names)
notebook_params["ds"] = '{{ ds }}'
notebook_params["next_ds"] = '{{ next_ds }}'
notebook_params["execution_date"] = '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}'
notebook_params["next_execution_date"] = '{{ next_execution_date.strftime("%Y-%m-%d %H:%M:%S") }}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('autoreport_washing',
    start_date=datetime(2022, 11, 28, tzinfo=Timezone("Asia/Seoul")),
    end_date=datetime(2022, 11, 29, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@hourly',
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