import os
import os
import requests
import boto3

from pathlib import Path
from airflow.models.connection import Connection
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from google.ads.googleads.client import GoogleAdsClient
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.providers.ssh.operators.ssh import SSHHook, SSHOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

from pendulum.tz.timezone import Timezone
from custom.operators import BaseUtils

_BASE_PATH = "/usr/local/airflow/dags"

# def _python_sensor_test():
#     """
#     A method for sensing files by python sensor operator
#
#     :return:
#     """
#     file_path = Path("/mnt/Project-시리즈/13. Auto Report/test/MMP/2304/2023-4-14_ALL.csv")
#
#     hook = SSHHook("ssh_default")
#     conn = hook.get_conn()
#     stdin, stdout, stderr = conn.exec_command("'/mnt/Project-시리즈/13. Auto Report/test/MMP/2304/'")
#     print(stdout.readlines())
#
#     return file_path.exists()

def _get_context_params(**kwargs):
    """
    A method for getting notebook params
    """

    print("======================== CHECK CONTEXT PARAMS ========================")
    print(BaseUtils.convert_pendulum_datetime_to_str(kwargs["data_interval_end"], time_zone="Asia/Seoul"))
    print(kwargs["ds"])
    print(kwargs["ts"])

    Variable.set("current_time", BaseUtils.convert_pendulum_datetime_to_str(kwargs["data_interval_end"]))

    print("======================== CHECK CONNECTION PARAMS ========================")
    conn = BaseHook.get_connection("slack_default")
    print(conn.host)
    print(conn.password)

def _check_data_interval(**kwargs):
    """
    A method for checking data interval
    """
    print("======================== CHECK VARIABLES ========================")
    print(Variable.get("current_time"))
    raise Exception("SLACK CONNECTION TEST")

    # ti = kwargs["ti"]
    # data_interval_end_date = BaseUtils.convert_pendulum_datetime_to_str(
    #     date=kwargs["data_interval_end"],
    #     format="%Y-%m-%d %H:%M:%S",
    #     time_zone="Asia/Seoul"
    # )
    # ti.xcom_push(key="data_interval_end", value=data_interval_end_date)

def _aws_conn_test():

    # (!) credentials based on root jinyoung.park
    # aws_access_key_id = 'AKIASPIODPUMQ36QKI5Q'
    # aws_secret_access_key = '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'

    # (!) credentials based on root account
    aws_access_key_id = "AKIASPIODPUMWJDWGQ4H"
    aws_secret_access_key = "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"

    client = boto3.client(
        'sns',
        region_name="ap-northeast-2",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    response = client.publish(
        # TopicArn='arn:aws:sns:ap-northeast-2:170217667865:Airflow_Test',
        TopicArn='arn:aws:sns:ap-northeast-2:170217667865:AirflowPubTest',
        # TargetArn='string',
        # PhoneNumber='string',
        Message='This is message publishing test from Airflow.',
    )

def _send_alarm_on_fail():
    """
    A method for sending error message to slack on fail tasks
    """

    # dag = context.get('task_instance').dag_id
    # task = context.get('task_instance').task_id
    # exec_date = context.get('data_interval_end')
    # exception = context.get('exception')
    # log_url = context.get('task_instance').log_url

    # message = f"""
    #     :red_circle: [TASK_FAILED]
    #     *DAG*: {dag}
    #     *TASK*: {task}
    #     *EXECUTION_TIME*: {exec_date}
    #     *EXCEPTION*: {exception}
    #     *ASSIGNEE*: jinyoung.park@ptbwa.com
    #     *LOG_URL*: {log_url}
    # """

    message = "test"



    # 1. using connection
    conn = BaseHook.get_connection("slack_default")
    url = f"{conn.host}/{conn.password}"
    icon_emoji = ":crying_cat_face:"
    channel = "# monitoring_airflow"
    payload = {
        "channel": channel,
        "username": "JYP",
        "text": message,
        "icon_emoji": icon_emoji
    }

    print("=============== REQUEST INFO ===============")
    print(conn.host)
    print(conn.password)
    print(payload)

    response = requests.post(url, json=payload)

    print(response.status_code)
    print(response.text)

    # 2. using operator
    # slack_webhook = SlackWebhookOperator(
    #     slack_webhook_conn_id="slack_default",
    #     task_id="alarm_fail_task",
    #     message=message
    # )
    #
    # return slack_webhook.execute(context)

def _send_alarm_on_success(context):
    """
    A method for sending complete message to slack on success dag
    """

    dag = context.get('task_instance').dag_id
    exec_date = context.get('data_interval_end')

    message = f"""
        :large_green_circle: [TASK_SUCCESS]
        *DAG*: {dag}
        *EXECUTION_TIME*: {exec_date}
        *ASSIGNEE*: jinyoung.park@ptbwa.com
    """

    url = "https://hooks.slack.com/services/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"
    # icon_emoji = ":crying_cat_face:"
    channel = "# monitoring_airflow"
    payload = {
        "channel": channel,
        "username": "JYP",
        "text": message,
        # "icon_emoji": icon_emoji
    }

    requests.post(url, json=payload)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    # 'on_failure_callback': _send_alarm_on_fail,
    # 'on_success_callback': _send_alarm_on_success
}

with DAG(os.path.basename(__file__).replace(".py", ""),
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["test"]
    ) as dag:

    env = "dev"
    project = "autoreport"

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # get_context_params = PythonOperator(
    #     task_id="get_context_params",
    #     python_callable=_get_context_params,
    #     op_kwargs={"env": env},
    #     trigger_rule=TriggerRule.ALL_SUCCESS,
    # )
    #
    # check_data_interval = PythonOperator(
    #     task_id="check_data_interval",
    #     python_callable=_check_data_interval,
    #     op_kwargs={"env": env},
    #     trigger_rule=TriggerRule.ALL_SUCCESS
    # )

    send_alarm_on_fail = PythonOperator(
        task_id="send_alarm_on_fail",
        python_callable=_send_alarm_on_fail,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    aws_conn_test = PythonOperator(
        task_id="aws_conn_test",
        python_callable=_aws_conn_test,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    #
    # series_gad_api = DatabricksRunNowOperator(
    #     task_id="series_gad_api",
    #     job_id="598982750753378",
    #     notebook_params={
    #         "env": env,
    #         "data_interval_end": "{{ ti.xcom_pull(task_ids='check_data_interval', key='data_interval_end') }}"
    #     },
    #     trigger_rule=TriggerRule.ALL_SUCCESS
    # )

    # (!) AWS SNS
    publish_message = SnsPublishOperator(
        aws_conn_id="aws_sns",
        task_id="publish_message",
        target_arn="arn:aws:sns:ap-northeast-2:170217667865:Airflow_Test",
        message="This is a sample message sent to SNS via an Apache Airflow DAG task.",
    )

    # python_sensor_test = PythonSensor(
    #     task_id="python_sensor_test",
    #     python_callable=_python_sensor_test,
    # )

    # start >> get_context_params >> check_data_interval >> series_gad_api >> end
    start >> send_alarm_on_fail >> aws_conn_test >> publish_message >> end



