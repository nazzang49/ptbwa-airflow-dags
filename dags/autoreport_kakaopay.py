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

kakaopay_fb_api_task = {
    'notebook_path': '/Shared/autoreport/kakaopay/kakaopay-fb',
}

kakaopay_gad_ono_api_task = {
    'notebook_path': '/Shared/autoreport/kakaopay/kakaopay-gad-ono',
}

kakaopay_gad_search_api_task = {
    'notebook_path': '/Shared/autoreport/kakaopay/kakaopay-gad-search',
}

kakaopay_kmt_api_task = {
    'notebook_path': '/Shared/autoreport/kakaopay/kakaopay-kmt',
}

kakaopay_kmt_keyword_api_task = {
    'notebook_path': '/Shared/autoreport/kakaopay/kakaopay-kmt-keyword',
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

with DAG('autoreport_kakaopay_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    kakaopay_fb_api_run = DatabricksSubmitRunOperator(
        task_id='kakaopay_fb_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=kakaopay_fb_api_task
    )

    kakaopay_gad_ono_api_run = DatabricksSubmitRunOperator(
        task_id='kakaopay_gad_ono_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kakaopay_gad_ono_api_task
    )

    kakaopay_gad_search_api_run = DatabricksSubmitRunOperator(
        task_id='kakaopay_gad_search_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kakaopay_gad_search_api_task
    )

    kakaopay_kmt_api_run = DatabricksSubmitRunOperator(
        task_id='kakaopay_kmt_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kakaopay_kmt_api_task
    )

    kakaopay_kmt_keyword_api_run = DatabricksSubmitRunOperator(
        task_id='kakaopay_kmt_keyword_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=kakaopay_kmt_keyword_api_task
    )

    start_run = DummyOperator(task_id="start")

    end_run = DummyOperator(task_id="end")

    start_run >> [kakaopay_fb_api_run, kakaopay_gad_ono_api_run, kakaopay_gad_search_api_run, kakaopay_kmt_api_run, kakaopay_kmt_keyword_api_run] >> end_run