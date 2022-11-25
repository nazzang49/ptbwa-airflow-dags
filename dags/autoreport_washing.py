from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

import os
import json

def create_notebook_params():
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
    return notebook_params

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

    create_notebook_params_run = PythonOperator(
        task_id="create_notebook_params_task",
        python_callable=create_notebook_params
    )

    washing_run = DatabricksRunNowOperator(
        task_id="washing_task",
        job_id="751730826324009",
        databricks_conn_id='databricks_default',
        notebook_params='{{ task_instance.xcom_pull(task_ids="create_notebook_params_task") }}'
    )

    start_run = DummyOperator(task_id="start")
    end_run = DummyOperator(task_id="end")

    start_run >> create_notebook_params_run >> washing_run >> end_run