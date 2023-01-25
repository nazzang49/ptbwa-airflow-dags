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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(f"{os.path.basename(__file__).replace('.py', '')}_api",
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    ) as dag_api:

    start = DummyOperator(task_id="start")
    trigger_sql_dag = TriggerDagRunOperator(
        task_id="trigger_sql_dag",
        trigger_dag_id=f"{os.path.basename(__file__).replace('.py', '')}_sql",
        trigger_run_id=None,
        execution_date="{{ ts }}",
        reset_dag_run=False,
        wait_for_completion=False,
        # poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    end = DummyOperator(task_id="end")

    start >> trigger_sql_dag >> end

with DAG(f"{os.path.basename(__file__).replace('.py', '')}_sql",
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    ) as dag_sql:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> end