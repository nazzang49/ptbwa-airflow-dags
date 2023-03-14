import airflow
import csv
import requests

from airflow.models import DagModel, Variable

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pytz import timezone

def get_bundle():
    bundle = list()
    with open('/tmp/bundle_list.csv' ,'r') as csvfile:        
        csvreader = csv.reader(csvfile)

        next(csvreader)

        for row in csvreader:
            bundle.append(row[0])
    
    bundle = list(set(bundle))
    
    print("===========================================")
    print("bundle: ", bundle)
    print("bundle length: ", len(bundle))
    Variable.set(key='bundle_list', value = bundle)
    Variable.set(key='bundle_len', value = len(bundle))

def unpause_dag(dag_id):
    dag = DagModel.get_dagmodel(dag_id)
    if dag is not None:
        dag.set_is_paused(is_paused = False)

def pause_dag(dag_id):
    dag = DagModel.get_dagmodel(dag_id)
    if dag is not None:
        dag.set_is_paused(is_paused = True)
        
def delete_variable(variable_key):
    for k in variable_key:
        Variable.delete(key=k)


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
    databricks_master = "tt_ice.app_master_nhn"
    databricks_info = "tt_ice.app_info_nhn"

    recawling_timedelta = relativedelta(months=3)
    recrawling_date = datetime.strftime(datetime.now(timezone('Asia/Seoul'))-recawling_timedelta, "%Y-%m-%d")
    
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
        
        UNION ALL
        
        SELECT 
            DISTINCT requestAppBundle 
        FROM 
            {databricks_info} 
        WHERE
            category01 IS NULL
    """

    Variable.set(key="query", value=query)
def send_alarm_on_success(context):
    """
    A method for sending complete message to slack on success tasks
    """
    dag = context.get('task_instance').dag_id
    task = context.get('task_instance').task_id
    exec_date = context.get('data_interval_end')

    message = f"""
        :large_green_circle: [TASK_SUCCESS]
        *DAG*: {dag}
        *TASK*: {task}
        *EXECUTION_TIME*: {exec_date}
        *ASSIGNEE*: hyeji.kim@ptbwa.com
    """
    
    url = "https://hooks.slack.com/services/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"
    # icon_emoji = ":crying_cat_face:"
    channel = "# monitoring_airflow"
    payload = {
        "channel": channel,
        "username": "CRAWLING_APP_INFO",
        "text": message,
        # "icon_emoji": icon_emoji
    }

    requests.post(url, json=payload)

    
def send_alarm_on_fail(context):
    """
    A method for sending error message to slack on fail tasks
    """

    dag = context.get('task_instance').dag_id
    task = context.get('task_instance').task_id
    exec_date = context.get('data_interval_end')
    exception = context.get('exception')
    log_url = context.get('task_instance').log_url

    message = f"""
        :red_circle: [TASK_FAILED]
        *DAG*: {dag}
        *TASK*: {task}
        *EXECUTION_TIME*: {exec_date}
        *EXCEPTION*: {exception}
        *ASSIGNEE*: hyeji.kim@ptbwa.com
        *LOG_URL*: {log_url}
    """

    # 1. using connection
    # conn = BaseHook.get_connection("slack_default")
    # url = f"{conn.host}/{conn.password}"
    url = "https://hooks.slack.com/services/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"
    # icon_emoji = ":crying_cat_face:"
    channel = "# monitoring_airflow"
    payload = {
        "channel": channel,
        "username": "CRAWLING_APP_INFO",
        "text": message,
        # "icon_emoji": icon_emoji
    }

    requests.post(url, json=payload)