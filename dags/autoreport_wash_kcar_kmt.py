import os.path
import yaml
import pendulum
import json
import pandas as pd

from airflow import DAG, AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

from utils import DagUtils
from custom.operators import AutoReportWashOperator, AutoReportValidationOperator, WashUtils
from datetime import timedelta

_BASE_PATH = "/usr/local/airflow/dags"
def _get_properties(**kwargs):
    """
    A method for getting common properties from local
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "properties", f"properties_{kwargs['env']}.yml")
    with open(file_path, "r", encoding="utf-8") as f:
        properties = yaml.load(f, Loader=yaml.FullLoader)

    ti.xcom_push(key="job_id", value=str(properties["job_id"]))
    ti.xcom_push(key="job_name", value=properties["job_name"])

def _get_notebook_params(**kwargs):
    """
    A method for getting notebook params from local
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", f"wash_kcar_kmt_{kwargs['env']}.json")
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params = json.load(f)

    ti.xcom_push(key="notebook_params", value=notebook_params)

def _get_total_period(**kwargs):
    """
    A method for getting total period before creating intervals
    """
    ti = kwargs["ti"]
    notebook_params = ti.xcom_pull(task_ids='get_notebook_params', key='notebook_params')
    scope = kwargs.pop("scope")
    s_date, e_date = WashUtils.calc_total_period(notebook_params, scope, **kwargs)

    total_period = {
        "s_date": s_date,
        "e_date": e_date,
    }

    print(f"[CHECK-TOTAL-PERIOD]{total_period}")
    ti.xcom_push(key="total_period", value=total_period)

def _get_validation_args(**kwargs):
    """
    A method for validating arguments
    """
    ti = kwargs["ti"]
    base_table_names = [
        "gad_kcar_wash_stat"
    ]

    table_names = [f"tt_{base_table_name}" if kwargs["env"] == "dev" else base_table_name for base_table_name in base_table_names]

    print(f"[CHECK-VALIDATION-TABLE-NAME]{table_names}")
    ti.xcom_push(key="table_names", value=",".join(table_names))

check_validation_notebook_task = {
    "notebook_path": "/Shared/validation/check-data-validation",
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    # schedule_interval='0 2 5 * *',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["auto_report", "kcar", "kmt", "wash"]
    ) as dag:

    env = "dev"
    intervals_len, washing_interval = DagUtils.get_intervals_len(
        adv="kcar",
        channel="kmt",
        env=env
    )

    if not intervals_len:
        raise AirflowException("[NOT-FOUND-INTERVALS]REQUIRED::{intervals_len}")

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    get_properties = PythonOperator(
        task_id="get_properties",
        python_callable=_get_properties,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    get_total_period = PythonOperator(
        task_id="get_total_period",
        python_callable=_get_total_period,
        op_kwargs={"scope": "washing"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    auto_report_wash_tasks = []
    for interval_idx in range(intervals_len):
        auto_report_wash_tasks.append(
            AutoReportWashOperator(
                task_id=f"auto_report_wash_{interval_idx}",
                job_id="{{ ti.xcom_pull(task_ids='get_properties', key='job_id') }}",
                databricks_conn_id="databricks_default",
                # trigger_rule=TriggerRule.ALL_DONE,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                interval_idx=interval_idx,
                interval_freq=f"{washing_interval}D",
                notebook_params="{{ ti.xcom_pull(task_ids='get_notebook_params', key='notebook_params') }}",
                total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
            )
        )

        if interval_idx == 0:
            start >> get_properties >> get_notebook_params >> get_total_period >> auto_report_wash_tasks[interval_idx]
        else:
            auto_report_wash_tasks[interval_idx - 1] >> auto_report_wash_tasks[interval_idx]

    get_validation_args = PythonOperator(
        task_id="get_validation_args",
        python_callable=_get_validation_args,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    check_validation = AutoReportValidationOperator(
        task_id="check_validation",
        notebook_task=check_validation_notebook_task,
        databricks_conn_id='databricks_default',
        existing_cluster_id="1026-083605-h88ik7f2",
        # trigger_rule=TriggerRule.ALL_DONE,
        total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
        table_names="{{ ti.xcom_pull(task_ids='get_validation_args', key='table_names') }}"
    )

    # start >> get_properties >> get_notebook_params >> get_total_period >> tg >> check_validation >> end
    auto_report_wash_tasks[-1] >> get_validation_args >> check_validation >> end
