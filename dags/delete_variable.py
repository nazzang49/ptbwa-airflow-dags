import airflow.utils.timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DagModel

from datetime import timedelta

default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "start_date" : airflow.utils.timezone.datetime(2023, 1, 18),
    "retries" : 10,
    "retry_delay" : timedelta(minutes=5),
}

with DAG(
     dag_id = "tt-delete_variable",
    # start_date = airflow.utils.timezone.datetsime(2023, 1, 18),
    default_args = default_args,
    schedule_interval = None
) as dag:

    def _delete_variable(variable_key):
        for k in variable_key:
            Variable.delete(key=k)

    def _unpause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = False)
    
    def _pause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = True)

    before_dag = DummyOperator(
        task_id = "tt-crawling_info"
    )

    delete_variable = PythonOperator(
        task_id = "tt-delete_variable",
        python_callable = _delete_variable,
        op_kwargs = {
            "variable_key" : ["bundle_list", "bundle_len"]
        }
    )

    pause_crawling_info_dag = PythonOperator(
        task_id = "tt-pause_crawling_info_dag",
        python_callable = _pause_dag,
        op_kwargs = {
            "dag_id" :"tt-crawling_info"
        }
    )

    pause_delete_variable_dag = PythonOperator(
        task_id = "tt-pause_delete_variable_dag",
        python_callable = _pause_dag,
        op_kwargs = {
            "dag_id" :"tt-delete_variable"
        }
    )

    end = DummyOperator(
        task_id = "end"
    )

    before_dag >> delete_variable >> pause_crawling_info_dag >> pause_delete_variable_dag >> end

