import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

#Define params for Submit Run Operator
# cluster = {
#     'spark_version': '7.3.x-scala2.12',
#     'num_workers': 2,
#     'node_type_id': 'i3.xlarge'
# }

ministock_fb_api_task = {
    'notebook_path': '/Shared/autoreport/ministock/ministock-fb',
}

ministock_asa_api_task = {
    'notebook_path': '/Shared/autoreport/ministock/ministock-asa',
}

ministock_gad_api_task = {
    'notebook_path': '/Shared/autoreport/ministock/ministock-gad',
}

ministock_tik_api_task = {
    'notebook_path': '/Shared/autoreport/ministock/ministock-tik',
}

#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(os.path.basename(__file__).replace(".py", ""),
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    ministock_gad_api_run = DatabricksSubmitRunOperator(
        task_id='ministock_gad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=ministock_gad_api_task
    )

    ministock_fb_api_run = DatabricksSubmitRunOperator(
        task_id='ministock_fb_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=ministock_fb_api_task
    )

    ministock_asa_api_run = DatabricksSubmitRunOperator(
        task_id='ministock_asa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=ministock_asa_api_task
    )

    ministock_tik_api_run = DatabricksSubmitRunOperator(
        task_id='ministock_tik_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=ministock_tik_api_task
    )

    # start_run = DummyOperator(task_id="start")
    #
    # end_run = DummyOperator(task_id="end")

    [ministock_gad_api_run, ministock_fb_api_run, ministock_asa_api_run, ministock_tik_api_task]