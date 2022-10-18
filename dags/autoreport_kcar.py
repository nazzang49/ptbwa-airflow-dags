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

kcar_ad_api_task = {
    'notebook_path': '/Shared/autoreport/kcar/kcar-ad',
}

kcar_gad_api_task = {
    'notebook_path': '/Shared/autoreport/kcar/kcar-gad',
}

kcar_kmt_api_task = {
    'notebook_path': '/Shared/autoreport/kcar/kcar-kmt',
}

kcar_nsa_api_task = {
    'notebook_path': '/Shared/autoreport/kcar/kcar-nsa',
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

with DAG('autoreport_kcar_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    kcar_ad_api_run = DatabricksSubmitRunOperator(
        task_id='kcar_ad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=kcar_ad_api_task
    )

    kcar_gad_api_run = DatabricksSubmitRunOperator(
        task_id='kcar_gad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kcar_gad_api_task
    )

    kcar_kmt_api_run = DatabricksSubmitRunOperator(
        task_id='kcar_kmt_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kcar_kmt_api_task
    )

    kcar_nsa_api_run = DatabricksSubmitRunOperator(
        task_id='kcar_nsa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kcar_nsa_api_task
    )

    start_run = EmptyOperator(task_id="start")

    end_run = EmptyOperator(task_id="end")

    start_run >> kcar_ad_api_run >> [kcar_gad_api_run, kcar_kmt_api_run, kcar_nsa_api_run] >> end_run