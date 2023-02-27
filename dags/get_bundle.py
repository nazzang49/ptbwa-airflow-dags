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

from user_function import unpause_dag, pause_dag


recawling_timedelta = relativedelta(months=3)
recrawling_date = datetime.strftime(datetime.now()-recawling_timedelta, "%Y-%m-%d")


default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    "retries" : 5
}

with DAG(
    dag_id = "tt-get_bundle",
    default_args = default_args,
    schedule_interval = None,
    start_date = datetime(2023, 2, 16, tzinfo=Timezone("Asia/Seoul")),
    catchup = False
    
) as dag:


    def get_bundle():
        bundle = list()
        with open('/tmp/bundle_list.csv' ,'r') as csvfile:        
            csvreader = csv.reader(csvfile)

            next(csvreader)

            for row in csvreader:
                bundle.append(row[0])

        print("===========================================")
        print("bundle: ", bundle)
        print("bundle length: ", len(bundle))
        Variable.set(key='bundle_list', value = bundle)
        Variable.set(key='bundle_len', value = len(bundle))


    before_dag = DummyOperator(
        task_id = "set_date_query_dag"
    )

    pause_set_date_query_dag = PythonOperator(
        task_id = "tt-pause_set_date_query_dag",
        python_callable = pause_dag,
        op_kwargs = {
            "dag_id" :"tt-set_date_query"
        }
    )

    connect_databricks_sql = DatabricksSqlOperator(
        task_id = "connect_databricks_sql",
        databricks_conn_id = "databricks_default",
        sql = [Variable.get(key="query")],
        output_path ='/tmp/bundle_list.csv',
        output_format = 'csv',
    )

    get_bundle_task = PythonOperator(
        task_id = 'tt-get_bundle',
        python_callable = get_bundle
    )

    unpause_crawling_info = PythonOperator(
        task_id = 'tt-unpause_crawling_info',
        python_callable = unpause_dag,
        op_kwargs = {
            "dag_id" : "tt-crawling_info"
        }
    )

    trigger_crawling_info_dag = TriggerDagRunOperator(
        task_id = "tt-trigger_crawling_info_dag",
        trigger_dag_id = "tt-crawling_info",
        
    )

    # next_dag = DummyOperator(
    #     task_id = "tt-crawling_info_dag"
    # )

    before_dag >> pause_set_date_query_dag >> connect_databricks_sql >> get_bundle_task >> unpause_crawling_info >> trigger_crawling_info_dag #>> next_dag

