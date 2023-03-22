import math

import airflow.utils.timezone

from airflow import DAG
from airflow.models import Variable, DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.operators.bash_operator import BashOperator

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from airflow.models import DagModel

from user_function import *

default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "start_date" : airflow.utils.timezone.datetime(2023, 1, 18),
    "retries" : 10,
    # "retry_delay" : timedelta(minutes=5),
    "retry_delay" : timedelta(seconds=5),
    "on_failure_callback" : send_alarm_on_fail,
    "on_success_callback" : send_alarm_on_success
}

with DAG(
    dag_id = "crawling_info",
    # start_date = airflow.utils.timezone.datetime(2023, 1, 18),
    default_args = default_args,
    schedule_interval = None
) as dag:


    before_dag = DummyOperator(
        task_id="get_bundle_dag"
    )

    pause_get_bundle_dag = PythonOperator(
        task_id = "pause_get_bundle_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"get_bundle"
        }
    )

    app_bundle_variable = Variable.get(key ='bundle_list')
    app_bundle_len = int(Variable.get(key ='bundle_len'))

    app_bundle_variable = app_bundle_variable[1:-1]
    app_bundle_variable = app_bundle_variable.replace("'", "")
    app_bundle_list = app_bundle_variable.split(', ')
    for idx, b in enumerate(app_bundle_list):
        app_bundle_list[idx] = b.strip()

    crawling_task_list = list()
    # batch_size = 2
    batch_num = 3
    batch_size = math.ceil(app_bundle_len/batch_num)
    
    task_idx = 0
    # for i, bundle in enumerate(app_bundle_list):        
    # for i in range(0, app_bundle_len, batch_size):
    # for i in range(app_bundle_len_variable):
    for i in range(0, app_bundle_len, batch_size):
        task_id = f"ssh_test_task_{task_idx}"
        globals()[f"ssh_test_task_{task_idx}"] = SSHOperator(
            # task_id = f"ssh_test_task_{i}",
            task_id = task_id,
            ssh_conn_id = "ssh_default",
            command = f"python /home/datateam/crawling/github/ptbwa-crawling-app-info/crawling_app_info.py --is_test 'True' --bundle_list '{app_bundle_list[i:i+batch_size]}' --task_idx {task_idx} --crawling_date '{Variable.get(key='crawling_date')}'"
        )

        crawling_task_list.append(globals()[f"ssh_test_task_{task_idx}"])
        task_idx += 1

    
    append_databricks = DatabricksRunNowOperator(
        task_id = "append_databricks",
        job_id = "856270508418368",
        notebook_params = {
            "crawling_date": Variable.get(key = "crawling_date"),
            "is_test": 'True'
        },
        trigger_rule = TriggerRule.ALL_SUCCESS
    )

#     kill_chrome_process = SSHOperator(
#         task_id = "tt-kill_chrome_process",
#         ssh_conn_id = "ssh_default",
#         command = "kill -9 `ps -ef|grep chrome|awk '{print $2}'`"
#     )

#     kill_chromedriver_process = SSHOperator(
#         task_id = "tt-kill_chromedriver_process",
#         ssh_conn_id = "ssh_default",
#         command = "kill -9 `ps -ef|grep chromedriver|awk '{print $2}'`"
#     )

    unpause_delete_variable_dag = PythonOperator(
        task_id = "unpause_delete_variable",
        python_callable = unpause_dag,
        op_kwargs = {
            "dag_id" :"delete_variable"
        }
    )

    trigger_delete_variable_dag = TriggerDagRunOperator(
        task_id = "trigger_delete_variable_dag",
        trigger_dag_id = "delete_variable",
    )


    before_dag >> pause_get_bundle_dag >> crawling_task_list >> append_databricks >> unpause_delete_variable_dag >>  trigger_delete_variable_dag 