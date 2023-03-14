import airflow.utils.timezone

from airflow import DAG
from airflow.models import Variable, DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DagModel

from datetime import timedelta

from user_function import *

default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "start_date" : airflow.utils.timezone.datetime(2023, 1, 18),
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5),
    "on_failure_callback" : send_alarm_on_fail,
    "on_success_callback" : send_alarm_on_success
}

with DAG(
     dag_id = "delete_variable",
    default_args = default_args,
    schedule_interval = None
) as dag:

    before_dag = DummyOperator(
        task_id = "crawling_info_dag"
    )

    delete_variable = PythonOperator(
        task_id = "delete_variable",
        python_callable = delete_variable,
        op_kwargs = {
            "variable_key" : ["bundle_list", "bundle_len", "query", "crawling_date", "crawling_since_date", "crawling_until_date"]
        }
    )

    pause_crawling_info_dag = PythonOperator(
        task_id = "pause_crawling_info_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"crawling_info"
        }
    )

    pause_delete_variable_dag = PythonOperator(
        task_id = "pause_delete_variable_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"delete_variable"
        }
    )

    # end = DummyOperator(
    #     task_id = "end"
    # )

    before_dag>> pause_crawling_info_dag >> delete_variable >> pause_delete_variable_dag # >> end

