from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

#Define params for Submit Run Operator
# cluster = {
#     'spark_version': '7.3.x-scala2.12',
#     'num_workers': 2,
#     'node_type_id': 'i3.xlarge'
# }

millie_af_raw_task = {
    'notebook_path': '/Shared/TO-BE-JOBS/daily_agg/appsflyer_raw_millie',
}

millie_af_mmp_task = {
    'notebook_path': '/Shared/autoreport/millie/appsflyer_millie_mmp_stat',
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

with DAG('autoreport_millie_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    millie_af_raw_run = DatabricksSubmitRunOperator(
        task_id='millie_af_raw_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=millie_af_raw_task
    )

    millie_af_mmp_run = DatabricksSubmitRunOperator(
        task_id='millie_af_mmp_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=millie_af_mmp_task
    )

    millie_af_raw_run >> millie_af_mmp_run