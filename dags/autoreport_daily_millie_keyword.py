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

millie_keyword_nsa_api_task = {
    'notebook_path': '/Shared/autoreport/millie/mille-nsa',
}

millie_keyword_nsa_second_api_task = {
    'notebook_path': '/Shared/autoreport/millie/mille-nsa-second',
}

millie_keyword_asa_api_task = {
    'notebook_path': '/Shared/autoreport/millie/mille-asa',
}

millie_keyword_gad_api_task = {
    'notebook_path': '/Shared/autoreport/millie/millie-gad',
}

millie_keyword_stat_sql_task = {
    'notebook_path': '/Shared/autoreport/millie/millie_keyword_stat_d',
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

with DAG('autoreport_millie_keyword_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    millie_keyword_nsa_api_run = DatabricksSubmitRunOperator(
        task_id='millie_keyword_nsa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=millie_keyword_nsa_api_task
    )

    millie_keyword_nsa_second_api_run = DatabricksSubmitRunOperator(
        task_id='millie_keyword_nsa_second_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=millie_keyword_nsa_second_api_task
    )

    millie_keyword_gad_api_run = DatabricksSubmitRunOperator(
        task_id='millie_keyword_gad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=millie_keyword_gad_api_task
    )

    millie_keyword_asa_api_run = DatabricksSubmitRunOperator(
        task_id='millie_keyword_asa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=millie_keyword_asa_api_task
    )

    millie_keyword_stat_sql_run = DatabricksSubmitRunOperator(
        task_id='millie_keyword_stat_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=millie_keyword_stat_sql_task
    )

    start_run = DummyOperator(task_id="start")

    end_run = DummyOperator(task_id="end")

    start_run >> [millie_keyword_nsa_api_run, millie_keyword_nsa_second_api_run, millie_keyword_asa_api_run, millie_keyword_gad_api_run] >> millie_keyword_stat_sql_run >> end_run