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

funble_airb_api_task = {
    'notebook_path': '/Shared/autoreport/funble/funble-airb',
}

funble_sql_task = {
    'notebook_path': '/Shared/autoreport/funble/funble_deduct_report_join',
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

with DAG('autoreport_funble_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    funble_airb_api_run = DatabricksSubmitRunOperator(
        task_id='funble_airb_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=funble_airb_api_task
    )

    funble_sql_run = DatabricksSubmitRunOperator(
        task_id='funble_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=funble_sql_task
    )

    start_run = DummyOperator(task_id="start")

    end_run = DummyOperator(task_id="end")

    start_run >> funble_airb_api_run >> funble_sql_run >> end_run