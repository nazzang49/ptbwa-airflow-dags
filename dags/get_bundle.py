# https://stackoverflow.com/questions/66820948/create-dynamic-workflows-in-airflow-with-xcom-value

import airflow.utils.timezone
import yaml
import csv

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, DagModel
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.utils.task_group import TaskGroup

from dateutil.relativedelta import relativedelta
from databricks import sql
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

# databricks_master = "ice.tt_google_play_store_master_nhn"

# today = datetime.strftime(datetime.now(), "%Y-%m-%d")
# yesterday = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
# crawling_since_date = yesterday
# crawling_until_date = yesterday

recawling_timedelta = relativedelta(months=3)
recrawling_date = datetime.strftime(datetime.now()-recawling_timedelta, "%Y-%m-%d")


default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "retries" : 5
    # "start_date" : airflow.utils.timezone.datetime(2023, 2, 15),
}

with DAG(
    dag_id = "tt-get_bundle",
    # start_date = airflow.utils.timezone.datetime(2023, 1, 18),
    default_args = default_args,
    # schedule_interval = "0 1 * * *",
    schedule_interval = None,
    # schedule_interval = "@daily",
    start_date = datetime(2023, 2, 16, tzinfo=Timezone("Asia/Seoul")),
    params = {
        "": ""
        },
    catchup = False
    
) as dag:

    def _unpause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = False)
    
    def _pause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = True)

    def get_bundle():
        bundle = list()
        with open('/tmp/bundle_list.csv' ,'r') as csvfile:        
            csvreader = csv.reader(csvfile)

            next(csvreader)

            for row in csvreader:
                bundle.append(row[0])

        # context['task_instance'].xcom_push(key="app_bundle", value=app_bundle)
        # context['task_instance'].xcom_push(key="app_bundle_length", value=len(app_bundle))
        print("===========================================")
        print("bundle: ", bundle)
        print("bundle length: ", len(bundle))
        Variable.set(key='bundle_list', value = bundle)
        Variable.set(key='bundle_len', value = len(bundle))
        # Variable.set(key="input_date", value = today)

        # return app_bundle

    before_dag = DummyOperator(
        task_id = "set_date_query"
    )

    pause_set_date_query_dag = PythonOperator(
        task_id = "tt-pause_set_date_query_dag",
        python_callable = _pause_dag,
        op_kwargs = {
            "dag_id" :"tt-set_date_query"
        }
    )

    connect_databricks_sql = DatabricksSqlOperator(
        task_id = "connect_databricks_sql",
        databricks_conn_id = "databricks_default",
        # sql_endpoint_name = "Starter Warehouse",
        sql = [Variable.get(key="query")],
        # sql = ["SELECT bundle FROM cream.propfit_request_hourly LIMIT 10;"],
        output_path ='/tmp/bundle_list.csv',
        # format_options={'header': 'true'},
        output_format = 'csv',
    )

    get_bundle_task = PythonOperator(
        task_id = 'tt-get_bundle',
        python_callable = get_bundle
    )

    unpause_crawling_info = PythonOperator(
        task_id = 'tt-unpause_crawling_info',
        python_callable = _unpause_dag,
        op_kwargs = {
            "dag_id" : "tt-crawling_info"
        }
    )

    trigger_crawling_info_dag = TriggerDagRunOperator(
        task_id = "tt-trigger_crawling_info_dag",
        trigger_dag_id = "tt-crawling_info",
        
    )

    next_dag = DummyOperator(
        task_id = "tt-crawling_info"
    )

    before_dag >> pause_set_date_query_dag >> connect_databricks_sql >> get_bundle_task >> unpause_crawling_info >> trigger_crawling_info_dag >> next_dag

