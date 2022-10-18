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

welrix_daily_api_task = {
    'notebook_path': '/Users/jinyoung.park@ptbwa.com/welrix-daily',
}

welrix_traffic_stat_sql_task = {
    'notebook_path': '/Shared/autoreport/welrix/welrix-traffic-stat',
}

welrix_groupby_stat_sql_task = {
    'notebook_path': '/Shared/autoreport/welrix/welrix-groupby-stat',
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

with DAG('autoreport_welrix_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    welrix_daily_api_run = DatabricksSubmitRunOperator(
        task_id='welrix_daily_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=welrix_daily_api_task
    )

    welrix_traffic_stat_sql_run = DatabricksSubmitRunOperator(
        task_id='welrix_traffic_stat_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=welrix_traffic_stat_sql_task
    )

    welrix_groupby_stat_sql_run = DatabricksSubmitRunOperator(
        task_id='welrix_groupby_stat_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=welrix_groupby_stat_sql_task
    )

    start_run = EmptyOperator(task_id="start")

    end_run = EmptyOperator(task_id="end")

    start_run >> welrix_daily_api_run >> welrix_traffic_stat_sql_run >> welrix_groupby_stat_sql_run >> end_run