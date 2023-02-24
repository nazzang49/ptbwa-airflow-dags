from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable, DagModel

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum.tz.timezone import Timezone

from user_function import * 

 
default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    # "retries" : 1
    # "start_date" : airflow.utils.timezone.datetime(2023, 2, 15),
}

with DAG(
    dag_id = "tt-set_input_date_query",
    # start_date = airflow.utils.timezone.datetime(2023, 1, 18),
    default_args = default_args,
    # schedule_interval = "0 1 * * *",
    schedule_interval = None,
    # schedule_interval = "@daily",
    start_date = datetime(2023, 2, 23, tzinfo=Timezone("Asia/Seoul")),
    catchup = False
    
) as dag: 
    # def _unpause_dag(dag_id):
    #     dag = DagModel.get_dagmodel(dag_id)
    #     dag.set_is_paused(is_paused = False)
 
    
    
    tt_set_input_specific_task = PythonOperator(
        task_id = "tt-set_nput_specific_task",
        python_callable = set_crawling_date_query,
        op_kwargs = {
            "crawling_date" : "{{dag_run.conf.crawling_date}}",
            "crawling_since_date" : "{{dag_run.conf.crawling_since_date}}",
            "crawling_until_date" : "{{dag_run.conf.crawling_until_date}}",
        }
    )

    tt_set_input_yesterday_task = PythonOperator(
        task_id = "tt-set_input_yesterday_task",
        python_callable = set_crawling_date_query,
        op_kwargs = {
            "crawling_date" : "{{dag_run.conf.crawling_date}}",
            "crawling_since_date" : None,
            "crawling_until_date" : None,
        },
        trigger_rule = TriggerRule.ALL_FAILED
    )
    
    unpause_get_bundle_dag = PythonOperator(
        task_id = "tt-unpause_get_bundle_dag",
        python_callable = unpause_dag,
        op_kwargs = {
            "dag_id" :"tt-get_bundle"
        },
        trigger_rule = TriggerRule.ALL_DONE
    )

    trigger_crawling_info_dag = TriggerDagRunOperator(
        task_id = "tt-trigger_get_bundle_dag",
        trigger_dag_id = "tt-get_bundle",
        # trigger_rule = TriggerRule.ALL_DONE  
    )

    next_dag = DummyOperator(
        task_id = "tt-get_bundle",
        trigger_rule = TriggerRule.ALL_DONE
    )

    tt_set_input_specific_task >> tt_set_input_yesterday_task >> unpause_get_bundle_dag >> trigger_crawling_info_dag >> next_dag





