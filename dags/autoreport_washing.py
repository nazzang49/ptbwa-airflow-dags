from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

import os
import json

notebook_params = dict()
notebook_names = list()
base_path = "./"
for config_file in os.listdir(base_path):
    notebook_name, file_ext = config_file.split(".")
    if file_ext == "json":
        notebook_names.append(notebook_name)
        with open(os.path.join(base_path, config_file), "r", encoding="utf-8") as f:
            notebook_params[notebook_name] = json.dumps(json.load(f))

notebook_params["notebook_names"] = ",".join(notebook_names)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('autoreport_washing',
    start_date=datetime(2022, 11, 24, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
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