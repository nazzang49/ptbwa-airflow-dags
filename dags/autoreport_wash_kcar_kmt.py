from airflow.decorators import dag, task, task_group
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

import pendulum
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    default_args=default_args,
    schedule_interval="0 2 5 * *",
    start_date=pendulum.datetime(2022, 11, 28, tz="Asia/Seoul"),
    tags=["autoreport", "kcar", "kmt"]
)
def autoreport_wash_kcar_kmt():

    @task
    def get_config(**kwargs):
        config = dict()
        file_path = "/usr/local/airflow/dags/kcar_kmt.json"
        with open(file_path, "r", encoding="utf-8") as f:
            config["washing_params"] = json.load(f)
            config["data_interval_end"] = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
        return config

    @task
    def create_info(config):
        current_date = datetime.strptime(config["data_interval_end"][:10], "%Y-%m-%d")

        if config["washing_params"]["washing_preset"]:
            if config["washing_params"]["washing_preset"] == "last_month":
                config["s_date"] = datetime(year=current_date.year, month=current_date.month - 1, day=1)
                config["e_date"] = datetime(year=current_date.year, month=current_date.month, day=1)
            elif config["washing_params"]["washing_preset"] == "last_week":
                config["s_date"] = current_date - timedelta(days=current_date.weekday(),
                                                            weeks=1)  # weeks=1 means "last_week_monday"
                config["e_date"] = current_date - timedelta(days=current_date.weekday(),
                                                            weeks=0)  # weeks=0 means "this_week_monday"
            else:
                raise ValueError(f"[INVALID-VALUE]APPLICABLE-WASHING-PRESET")
        else:
            s_minus, e_minus = config["washing_params"]["washing_period"].split("|")
            config["s_date"] = current_date - timedelta(days=int(s_minus))
            config["e_date"] = current_date - timedelta(days=int(e_minus))

        config["s_date"] = config["s_date"].strftime("%Y-%m-%d")
        config["e_date"] = config["e_date"].strftime("%Y-%m-%d")

        return config

    @task_group
    def wash_group(config):
        data_interval_end = config["data_interval_end"]
        print(data_interval_end)

        wash_tasks = list()
        for i in range(10):
            wash_task = DatabricksRunNowOperator(
                task_id=f"wash_task_{i}",
                job_id="751730826324009",
                databricks_conn_id='databricks_default',
                notebook_params={"config": "config"},
            )
            wash_tasks.append(wash_task)
        return wash_tasks

    @task
    def end(results):
        print(f'END')

    return end(wash_group(create_info(get_config())))

autoreport_wash_kcar_kmt = autoreport_wash_kcar_kmt()