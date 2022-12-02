from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
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

def get_config(**kwargs):
    config = dict()
    file_path = "/usr/local/airflow/dags/kcar_kmt.json"
    with open(file_path, "r", encoding="utf-8") as f:
        config["washing_params"] = json.dumps(json.load(f))
        config["data_interval_start"] = kwargs["data_interval_start"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
        config["data_interval_end"] = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    return config

def add_params(idx, **kwargs):
    ti = kwargs["ti"]
    config = ti.xcom_pull(task_ids="get_config", key="return_value")
    config["n_group"] = idx
    return config

with DAG('autoreport_wash_kcar_kmt_1',
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    schedule_interval='0 2 5 * *',
    catchup=False,
    default_args=default_args
    ) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_config_task = PythonOperator(
        task_id="get_config",
        python_callable=get_config,
    )

    with TaskGroup(group_id="wash_group") as tg:
        wash_tasks = []
        add_params_tasks = []
        for i in range(10):
            add_params_tasks.append(PythonOperator(
                task_id=f"add_params_{i}",
                python_callable=add_params,
                op_args=[i]
            ))

            wash_tasks.append(
                DatabricksRunNowOperator(
                    task_id=f"wash_task_{i}",
                    job_id="751730826324009",
                    databricks_conn_id="databricks_default",
                    notebook_params=f"{{{{ ti.xcom_pull(task_ids='add_params_{i}' key='return_value') }}}}"
                )
            )

            if i == 0:
                add_params_tasks[i] >> wash_tasks[i]
            else:
                wash_tasks[i - 1] >> add_params_tasks[i] >> wash_tasks[i]

    start >> get_config_task >> tg >> end