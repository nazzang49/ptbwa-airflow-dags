from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from custom.AutoReportWashOperator import AutoReportWashOperator
from datetime import timedelta

import pendulum
import json

from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def _get_config(**kwargs):
    config = dict()
    file_path = "/usr/local/airflow/dags/kcar_kmt.json"
    with open(file_path, "r", encoding="utf-8") as f:
        config["washing_params"] = json.load(f)
    return config

with DAG('autoreport_wash_kcar_kmt_2',
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    schedule_interval='0 2 5 * *',
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True
    ) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_config_task = PythonOperator(
        task_id="get_config",
        python_callable=_get_config,
    )

    with TaskGroup(group_id="wash_group") as tg:
        auto_report_wash_tasks = []
        for i in range(10):
            auto_report_wash_tasks.append(
                AutoReportWashOperator(
                    task_id=f"auto_report_wash_{i}",
                    job_id="751730826324009",
                    databricks_conn_id="databricks_default",
                    notebook_params="{{ ti.xcom_pull(task_ids='get_config', key='return_value') }}",
                    n_interval=i,
                    d_interval=3
                )
            )

            if i > 0:
                auto_report_wash_tasks[i - 1] >> auto_report_wash_tasks[i]

    start >> get_config_task >> tg >> end