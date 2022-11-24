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

test_task = {
    'notebook_path': '/Shared/airflow/test',
}

#Define params for Run Now Operator
notebook_params = {
    "Variable": "5",
    "test_time": datetime.now().strftime("%Y-%m-%d")
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG('test_dag',
    start_date=datetime(2022, 11, 23, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
    ) as dag:

    test_run = DatabricksSubmitRunOperator(
        task_id='test_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="1026-083605-h88ik7f2",     # All-Purpose Cluster
        notebook_task=test_task,
        notebook_params=notebook_params
    )

    test_run