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

pcar_af_api_task = {
    'notebook_path': '/Shared/autoreport/pcar/appsflyer_raw_peoplecar',
}

pcar_gsheet_api_task = {
    'notebook_path': '/Shared/autoreport/pcar/gsheet_peoplecar_scheduler',
}

pcar_stat_sql_task = {
    'notebook_path': '/Shared/autoreport/pcar/appsflyer_stat_peoplecar_d',
}

pcar_keyword_asa_api_task = {
    'notebook_path': '/Shared/autoreport/pcar/pcar-asa',
}

pcar_keyword_nsa_mo_api_task = {
    'notebook_path': '/Shared/autoreport/pcar/pcar-nsa-Mo',
}

pcar_keyword_nsa_rent_api_task = {
    'notebook_path': '/Shared/autoreport/pcar/pcar-nsa-rent',
}

pcar_keyword_stat_d_sql_task = {
    'notebook_path': '/Shared/autoreport/pcar/peoplecar_keyword_stat_d',
}

pcar_keyword_stat_w_sql_task = {
    'notebook_path': '/Shared/autoreport/pcar/peoplecar_keyword_stat_w',
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

with DAG('autoreport_pcar_dag',
    start_date=datetime(2022, 10, 17, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    pcar_af_api_run = DatabricksSubmitRunOperator(
        task_id='pcar_af_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",     # All-Purpose Cluster
        notebook_task=pcar_af_api_task
    )

    pcar_gsheet_api_run = DatabricksSubmitRunOperator(
        task_id='pcar_gsheet_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_gsheet_api_task
    )

    pcar_stat_sql_run = DatabricksSubmitRunOperator(
        task_id='pcar_stat_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_stat_sql_task
    )

    pcar_keyword_asa_api_run = DatabricksSubmitRunOperator(
        task_id='pcar_keyword_asa_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_keyword_asa_api_task
    )

    pcar_keyword_nsa_rent_api_run = DatabricksSubmitRunOperator(
        task_id='pcar_keyword_nsa_rent_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_keyword_nsa_rent_api_task
    )

    pcar_keyword_nsa_mo_api_run = DatabricksSubmitRunOperator(
        task_id='pcar_keyword_nsa_mo_api_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_keyword_nsa_mo_api_task
    )

    pcar_keyword_stat_d_sql_run = DatabricksSubmitRunOperator(
        task_id='pcar_keyword_stat_d_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_keyword_stat_d_sql_task
    )

    pcar_keyword_stat_w_sql_run = DatabricksSubmitRunOperator(
        task_id='pcar_keyword_stat_w_sql_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id="0711-132151-yfw708gh",  # All-Purpose Cluster
        notebook_task=pcar_keyword_stat_w_sql_task
    )

    [pcar_af_api_run, pcar_gsheet_api_run, pcar_keyword_asa_api_run, pcar_keyword_nsa_mo_api_run, pcar_keyword_nsa_rent_api_run] >> [pcar_stat_sql_run, pcar_keyword_stat_d_sql_run, pcar_keyword_stat_w_sql_run]