import os.path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from jinja2 import Environment, FileSystemLoader
from custom.operators import AutoReportWashOperator
from datetime import timedelta

import yaml
import pendulum
import json

from airflow.utils.task_group import TaskGroup

_base_path = "/usr/local/airflow/dags"
def _get_properties(**kwargs):
    """
    A method for getting common properties from local
    :return:
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_base_path, f"properties_{kwargs['env']}.yml")
    with open(file_path, "r", encoding="utf-8") as f:
        properties = yaml.load(f, Loader=yaml.FullLoader)

    ti.xcom_push(key="job_id", value=str(properties["job_id"]))
    ti.xcom_push(key="job_name", value=str(properties["job_name"]))

def _get_notebook_params(env):
    """
    A method for getting notebook params from local
    :return:
    """
    notebook_params = dict()
    file_path = f"/usr/local/airflow/dags/kcar_gad_{env}.json"
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params["washing_params"] = json.load(f)
    return notebook_params

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('autoreport_wash_kcar_gad',
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    # schedule_interval='0 2 5 * *',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["autoreport", "kcar", "gad", "wash"]
    ) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": "dev"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    get_properties = PythonOperator(
        task_id="get_properties",
        python_callable=_get_properties,
        op_kwargs={"env": "dev"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # (!) last_week
    # with TaskGroup(group_id="wash_group") as tg:
    #     auto_report_wash_tasks = []
    #     for i in range(2):
    #         auto_report_wash_tasks.append(
    #             AutoReportWashOperator(
    #                 task_id=f"auto_report_wash_{i}",
    #                 job_id="751730826324009",
    #                 databricks_conn_id="databricks_default",
    #                 notebook_params="{{ti.xcom_pull(task_ids='get_notebook_params', key='return_value')}}",
    #                 n_interval=i,
    #                 d_interval=3
    #             )
    #         )
    #
    #         if i > 0:
    #             auto_report_wash_tasks[i - 1] >> auto_report_wash_tasks[i]

    # (!) last_month
    with TaskGroup(group_id="wash_group") as tg:
        auto_report_wash_tasks = []
        for i in range(10):
            auto_report_wash_tasks.append(
                AutoReportWashOperator(
                    task_id=f"auto_report_wash_{i}",
                    job_id="{{ti.xcom_pull(task_ids='get_properties', key='job_id')}}",
                    databricks_conn_id="databricks_default",
                    notebook_params="{{ti.xcom_pull(task_ids='get_notebook_params', key='return_value')}}",
                    trigger_rule=TriggerRule.ALL_DONE,
                    interval_idx=i,
                    interval_freq="3D",
                )
            )

            if i > 0:
                auto_report_wash_tasks[i - 1] >> auto_report_wash_tasks[i]

    start >> get_properties >> get_notebook_params >> tg >> end