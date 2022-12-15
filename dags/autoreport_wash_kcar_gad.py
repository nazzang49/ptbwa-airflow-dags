import os.path
import yaml
import pendulum
import json
import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from custom.operators import AutoReportWashOperator, AutoReportValidationOperator, Utils
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
    file_path = os.path.join(_BASE_PATH, "configs", f"kcar_gad_{kwargs['env']}.json")
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
    s_date, e_date = Utils.calc_total_period(notebook_params, scope, **kwargs)

    total_period = {
        "s_date": s_date,
        "e_date": e_date,
    }

    print(f"[CHECK-TOTAL-PERIOD]{total_period}")
    ti.xcom_push(key="total_period", value=total_period)

def _get_validation_args(**kwargs):
    ti = kwargs["ti"]
    base_table_names = [
        "gad_kcar_general_wash_stat",
        "gad_kcar_keyword_wash_stat"
    ]

    table_names = [
        f"tt_{base_table_name}" if kwargs["env"] == "dev" else base_table_name for base_table_name in base_table_names
    ]

    print(f"[CHECK-VALIDATION-TABLE-NAME]{table_names}")
    ti.xcom_push(key="table_name", value=",".join(table_names))

check_validation_notebook_task = {
    "notebook_path": "/Shared/validation/check-data-validation",
}

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
    tags=["auto_report", "kcar", "gad", "wash"]
    ) as dag:

    env = "dev"
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

    auto_report_wash = AutoReportWashOperator(
        task_id=f"auto_report_wash",
        job_id="{{ ti.xcom_pull(task_ids='get_properties', key='job_id') }}",
        databricks_conn_id="databricks_default",
        # trigger_rule=TriggerRule.ALL_DONE,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        notebook_params="{{ ti.xcom_pull(task_ids='get_notebook_params', key='notebook_params') }}",
        total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
        is_interval=False
    )

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
        table_name="{{ ti.xcom_pull(task_ids='get_validation_args', key='table_name') }}"
    )

    start >> get_properties >> get_notebook_params >> get_total_period >> auto_report_wash >> check_validation >> end
