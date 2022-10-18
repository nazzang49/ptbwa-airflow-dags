from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

#Define params for Submit Run Operator
# cluster = {
#     'spark_version': '7.3.x-scala2.12',
#     'num_workers': 2,
#     'node_type_id': 'i3.xlarge'
# }

latam_fb_api_task = {
    'notebook_path': '/Shared/autoreport/latam/latam-fb',
}

latam_gad_api_task = {
    'notebook_path': '/Shared/autoreport/latam/latam-gad',
}

latam_tik_api_task = {
    'notebook_path': '/Shared/autoreport/latam/latam-tik',
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

with DAG('autoreport_latam_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    latam_fb_api_run = DatabricksSubmitRunOperator(
        task_id='latam_fb_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=latam_fb_api_task
    )

    latam_gad_api_run = DatabricksSubmitRunOperator(
        task_id='latam_gad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=latam_gad_api_task
    )

    latam_tik_api_run = DatabricksSubmitRunOperator(
        task_id='latam_tik_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=latam_tik_api_task
    )

    start_run = EmptyOperator(task_id="start")

    end_run = EmptyOperator(task_id="end")

    start_run >> [latam_gad_api_run, latam_tik_api_run, latam_fb_api_task] >> end_run