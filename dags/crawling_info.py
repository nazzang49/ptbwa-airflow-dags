import math

import airflow.utils.timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import timedelta
from airflow.models import DagModel

default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "start_date" : airflow.utils.timezone.datetime(2023, 1, 18),
    "retries" : 5,
    # "retry_delay" : timedelta(minutes=5),
    "retry_delay" : timedelta(seconds=5),
}

with DAG(
    dag_id = "tt-crawling_info",
    # start_date = airflow.utils.timezone.datetime(2023, 1, 18),
    default_args = default_args,
    schedule_interval = None
) as dag:

    def print_all(bundle):
        print("===========================================")
        print(bundle)
    
    def _unpause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = False)

    before_dag = DummyOperator(
        task_id="tt-get_bundle"
    )

    app_bundle_variable = Variable.get(key ='bundle_list')
    app_bundle_len = int(Variable.get(key ='bundle_len'))

    app_bundle_variable = app_bundle_variable[1:-1]
    app_bundle_variable = app_bundle_variable.replace("'", "")
    app_bundle_list = app_bundle_variable.split(', ')

    cralwing_task_list = list()
    # batch_size = 2
    batch_num = 3
    batch_size = math.ceil(app_bundle_len/batch_num)
    
    task_idx = 0
    # for i, bundle in enumerate(app_bundle_list):        
    # for i in range(0, app_bundle_len, batch_size):
    # for i in range(app_bundle_len_variable):
    for i in range(0, app_bundle_len, batch_size):
        task_id = f"tt-ssh_test_task_{task_idx}"
        globals()[f"ssh_test_task_{task_idx}"] = SSHOperator(
            # task_id = f"tt-ssh_test_task_{i}",
            task_id = task_id,
            ssh_conn_id = "ssh_default",
            # command = f"echo '{app_bundle_list[i:i+batch_size]}'"
            command = f"python /home/datateam/crawling/github/ptbwa-crawling-app-info/crawling_app_info.py --is_test 'False' --bundle_list '{app_bundle_list[i:i+batch_size]}' --task_idx {task_idx}"
            # command = "echo 'Hello'"
        )
        
        # c = PythonOperator(
        #     task_id = f"tt-crawling_task_{i}",
        #     python_callable = print_all,
        #     op_kwargs = {
        #         "bundle" : bundle
        #     },
        # )
        # cralwing_task_list.append(globals()[f"crawling_task_{i}"])
        cralwing_task_list.append(globals()[f"ssh_test_task_{task_idx}"])
        task_idx += 1

    unpause_delete_variable_dag = PythonOperator(
        task_id = "tt-unpause_delete_variable_dag",
        python_callable = _unpause_dag,
        op_kwargs = {
            "dag_id" : "tt-delete_variable"
        }
    )


    trigger_delete_variable_dag = TriggerDagRunOperator(
        task_id = "tt-trigger_dag2",
        trigger_dag_id = "tt-delete_variable",
    )

    next_dag = DummyOperator(
        task_id = "tt-delete_variable"
    )

    before_dag >> cralwing_task_list >> unpause_delete_variable_dag >> trigger_delete_variable_dag >> next_dag