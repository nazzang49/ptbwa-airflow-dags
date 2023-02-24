from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable, DagModel

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum.tz.timezone import Timezone

 
default_args={
    "owner" : "hyeji",
    "provide_context" : True,
    "depends_on_past" : False,
    # "retries" : 1
    # "start_date" : airflow.utils.timezone.datetime(2023, 2, 15),
}

with DAG(
    dag_id = "tt-set_date_query",
    # start_date = airflow.utils.timezone.datetime(2023, 1, 18),
    default_args = default_args,
    # schedule_interval = "0 1 * * *",
    schedule_interval = None,
    # schedule_interval = "@daily",
    start_date = datetime(2023, 2, 23, tzinfo=Timezone("Asia/Seoul")),
    catchup = False
    
) as dag: 
    def _unpause_dag(dag_id):
        dag = DagModel.get_dagmodel(dag_id)
        dag.set_is_paused(is_paused = False)
 
    def set_crawling_date_query(crawling_date, crawling_since_date, crawling_until_date):
        crawling_date, crawling_since_date, crawling_until_date = set_crawling_date(crawling_date, crawling_since_date, crawling_until_date)
        set_query(crawling_date, crawling_since_date, crawling_until_date)
        
        
    def set_crawling_date(crawling_date, crawling_since_date, crawling_until_date):        
        if crawling_since_date is None and crawling_until_date is None:
            crawling_since_date = datetime.strftime(datetime.strptime(crawling_date, "%Y-%m-%d")-timedelta(days=1), "%Y-%m-%d")
            crawling_until_date = datetime.strftime(datetime.strptime(crawling_date, "%Y-%m-%d")-timedelta(days=1), "%Y-%m-%d")        
        
        Variable.set(key="crawling_date", value=crawling_date)
        Variable.set(key="crawling_since_date", value=crawling_since_date)
        Variable.set(key="crawling_until_date", value=crawling_until_date)

        print("date: ", crawling_date)
        print("since date: ", crawling_since_date)
        print("until date: ", crawling_until_date)

        return crawling_date, crawling_since_date, crawling_until_date


    def set_query(crawling_date, crawling_since_date, crawling_until_date):
        databricks_master = "ice.tt_google_play_store_master_nhn"
        databricks_info = "ice.tt_google_play_store_info_nhn"

        recawling_timedelta = relativedelta(months=3)
        recrawling_date = datetime.strftime(datetime.now()-recawling_timedelta, "%Y-%m-%d")
        
        query = f"""
            SELECT 
                requestAppBundle
            FROM
                (
                    -- 수집한 앱 번들 - 기존에 수집한 앱 번들
                    SELECT
                        distinct split(bundle, '&')[0] as requestAppBundle 
                    FROM 
                        hive_metastore.cream.propfit_request_hourly
                    WHERE 
                        date(actiontime_local_h) BETWEEN '{crawling_since_date}' AND '{crawling_until_date}'

                    MINUS              

                    SELECT 
                        requestAppBundle
                    FROM
                        {databricks_master}
                )
            -- 일정 기간이 지나고, 이전에 정상적으로 수집했으면 재 크롤링

            UNION ALL
            
            SELECT
                requestAppBundle
            FROM 
                {databricks_master}                   
            WHERE
                inputDate <= {recrawling_date}
                AND flag=200

            -- status code가 429인 경우 재 크롤링
                
            UNION ALL
            
            SELECT
                requestAppBundle
            FROM 
                {databricks_master}                   
            WHERE
                flag=429

             -- 앱 이름이나 설명이 없는 경우 재 크롤링
            
            UNION ALL
            
            SELECT
                requestAppBundle
            FROM
                {databricks_info}
            WHERE 
                requestAppName IS NULL
                OR description IS NULL               
        """

        Variable.set(key="query", value=query)
    
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

    tt_set_yesterday_task = PythonOperator(
        task_id = "tt-set_yesterday_task",
        python_callable = set_crawling_date_query,
        op_kwargs = {
            "crawling_date" : datetime.strftime(datetime.now(), "%Y-%m-%d"),
            "crawling_since_date" : None,
            "crawling_until_date" : None,
        },
        trigger_rule = TriggerRule.ALL_FAILED
    )

    # tt_set_query = PythonOperator(
    #     task_id = "tt-set_query",
    #     python_callable = set_query,
    #     op_kwargs = {
    #         "crawling_date" : Variable.get(key="crawling_date"),
    #         "crawing_since_date" : Variable.get(key="crawing_since_date"),
    #         "crawing_until_date" : Variable.get(key="crawing_until_date")
    #     },
    #     trigger_rule = TriggerRule.ONE_FAILED
    # )

    
    unpause_get_bundle_dag = PythonOperator(
        task_id = "tt-unpause_get_bundle_dag",
        python_callable = _unpause_dag,
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

    tt_set_input_specific_task >> tt_set_input_yesterday_task >> tt_set_yesterday_task >> unpause_get_bundle_dag >> trigger_crawling_info_dag >> next_dag





