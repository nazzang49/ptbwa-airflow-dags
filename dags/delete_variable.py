import airflow.utils.timezone

from airflow import DAG
from airflow.models import Variable, DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DagModel

from datetime import timedelta

from user_function import unpause_dag, pause_dag

default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "start_date" : airflow.utils.timezone.datetime(2023, 1, 18),
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5),
}

with DAG(
     dag_id = "tt-delete_variable",
    default_args = default_args,
    schedule_interval = None
) as dag:

    def _delete_variable(variable_key):
        for k in variable_key:
            Variable.delete(key=k)


    before_dag = DummyOperator(
        task_id = "tt-crawling_info_dag"
    )

    delete_variable = PythonOperator(
        task_id = "tt-delete_variable",
        python_callable = _delete_variable,
        op_kwargs = {
            "variable_key" : ["bundle_list", "bundle_len", "query", "crawling_date", "crawling_since_date", "crawling_until_date"]
        }
    )

    pause_crawling_info_dag = PythonOperator(
        task_id = "tt-pause_crawling_info_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"tt-crawling_info"
        }
    )

    pause_delete_variable_dag = PythonOperator(
        task_id = "tt-pause_delete_variable_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"tt-delete_variable"
        }
    )

    end = DummyOperator(
        task_id = "end"
    )

    before_dag>> pause_crawling_info_dag >> delete_variable >> pause_delete_variable_dag >> end


