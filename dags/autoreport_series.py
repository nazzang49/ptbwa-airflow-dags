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

series_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-ad',
}

series_gad_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-gad',
}

series_fb_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-fb',
}

series_twt_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-twt',
}

series_asa_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-asa',
}

series_tik_api_task = {
    'notebook_path': '/Shared/autoreport/series/series-tik',
}

series_stat_sql_task = {
    'notebook_path': '/Shared/autoreport/series/series-ad-stat',
}

series_report_sql_task = {
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

    series_api_run = DatabricksSubmitRunOperator(
        task_id='series_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=series_api_task
    )

    series_stat_sql_run = DatabricksSubmitRunOperator(
        task_id='series_stat_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_stat_sql_task
    )

    series_gad_api_run = DatabricksSubmitRunOperator(
        task_id='series_gad_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_gad_api_task
    )

    series_fb_api_run = DatabricksSubmitRunOperator(
        task_id='series_fb_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_fb_api_task
    )

    series_twt_api_run = DatabricksSubmitRunOperator(
        task_id='series_twt_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_twt_api_task
    )

    series_asa_api_run = DatabricksSubmitRunOperator(
        task_id='series_asa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_asa_api_task
    )

    series_tik_api_run = DatabricksSubmitRunOperator(
        task_id='series_tik_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_tik_api_task
    )

    series_report_sql_run = DatabricksSubmitRunOperator(
        task_id='series_report_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=series_report_sql_task
    )

    series_api_run >> series_stat_sql_run
    series_stat_sql_run >> [series_gad_api_run, series_fb_api_run, series_twt_api_run, series_asa_api_run, series_tik_api_run]
    [series_gad_api_run, series_fb_api_run, series_twt_api_run, series_asa_api_run, series_tik_api_run] >> series_report_sql_run