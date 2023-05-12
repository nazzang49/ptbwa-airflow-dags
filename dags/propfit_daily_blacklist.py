import os
import json
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pendulum.tz.timezone import Timezone
from custom.operators import AutoReportValidationOperator, DailyUtils, BaseUtils

_BASE_PATH = "/usr/local/airflow/dags"
def _get_notebook_params(**kwargs):
    """
    A method for getting notebook params
    """
    ti = kwargs["ti"]

    print("=========================CHECK_ENV_PARAMS=========================")
    print(f"{kwargs['env']}")


    file_path = os.path.join(_BASE_PATH, "configs", "propfit", f"blacklist.json")
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params = json.load(f)

    for k, v in notebook_params.items():
        ti.xcom_push(key=k, value=v)

# def _get_validation_config(**kwargs):
#     """
#     A method for getting data validation config
#     """
#     ti = kwargs["ti"]
#     file_path = os.path.join(_BASE_PATH, "configs", "databricks", "validation_config.json")
#     with open(file_path, "r", encoding="utf-8") as f:
#         validation_config = json.load(f)
#
#     print(f"[CHECK-VALIDATION-CONFIG]{validation_config}")
#     ti.xcom_push(key="validation_config", value=validation_config)
#
#     # (!) changeable
#     table_names = [
#         "fb_funble_ad_stats",
#     ]
#
#     print(f"[CHECK-VALIDATION-TABLE-NAME]{table_names}")
#     ti.xcom_push(key="table_names", value=",".join(table_names))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

################################### API ###################################


with DAG(f"{os.path.basename(__file__).replace('.py', '')}",
    start_date=datetime(2023, 5, 12, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["propfit", "blacklist", "domain", "daily"]
    ) as dag_api:

    env = "dev"
    project = "propfit"

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    with TaskGroup(group_id="main_tasks") as main_tasks:
        main_tasks_start = DummyOperator(
            task_id="main_tasks_start",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        propfit_blacklist_api = DatabricksRunNowOperator(
            task_id="propfit_blacklist_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='job_id') }}",
            notebook_params={
                "env": env,
                "start_date": "2023-03-01",
                "end_date": "2023-03-31",
                "advertiser_idx": "5",
                "under_threshold": "2000",
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        main_tasks_start >> propfit_blacklist_api

    ############################## (!) dag ##############################
    start >> get_notebook_params >> main_tasks >> end







