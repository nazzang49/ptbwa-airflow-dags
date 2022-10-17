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

series_api_task_1 = {
    'notebook_path': '/Shared/autoreport/series/series-ad',
}

series_api_task_2 = {
    'notebook_path': '/Shared/autoreport/series/series-gad',
}

series_api_task_3 = {
    'notebook_path': '/Shared/autoreport/series/series-fb',
}

series_api_task_4 = {
    'notebook_path': '/Shared/autoreport/series/series-twt',
}

series_api_task_5 = {
    'notebook_path': '/Shared/autoreport/series/series-asa',
}

series_api_task_6 = {
    'notebook_path': '/Shared/autoreport/series/series-tik',
}

series_sql_task_1 = {
    'notebook_path': '/Shared/autoreport/series/series-ad-stat',
}

series_sql_task_2 = {
    'notebook_path': '/Shared/autoreport/series/series-report-join',
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

with DAG('autoreport_series_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    series_api_run_1 = DatabricksSubmitRunOperator(
        task_id='series_api_task_1',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",     # All-Purpose Cluster
        notebook_task=series_api_task_1
    )

    series_sql_run_1 = DatabricksSubmitRunOperator(
        task_id='series_sql_task_1',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_sql_task_1
    )

    series_api_run_2 = DatabricksSubmitRunOperator(
        task_id='series_api_task_2',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_api_task_2
    )

    series_api_run_3 = DatabricksSubmitRunOperator(
        task_id='series_api_task_3',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_api_task_3
    )

    series_api_run_4 = DatabricksSubmitRunOperator(
        task_id='series_api_task_4',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_api_task_4
    )

    series_api_run_5 = DatabricksSubmitRunOperator(
        task_id='series_api_task_5',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_api_task_5
    )

    series_api_run_6 = DatabricksSubmitRunOperator(
        task_id='series_api_task_6',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_api_task_6
    )

    series_sql_run_2 = DatabricksSubmitRunOperator(
        task_id='series_sql_task_2',
        databricks_conn_id='databricks_default',
        existing_cluster_id="test",  # All-Purpose Cluster
        notebook_task=series_sql_task_2
    )

    series_api_run_1 >> series_sql_run_1
    series_sql_run_1 >> [series_api_run_2, series_api_run_3, series_api_run_4, series_api_run_5, series_api_run_6]
    [series_api_run_2, series_api_run_3, series_api_run_4, series_api_run_5, series_api_run_6] >> series_sql_run_2