import os
import json

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pendulum.tz.timezone import Timezone
from custom.operators import AutoReportValidationOperator, DailyUtils

_BASE_PATH = "/usr/local/airflow/dags"
def _get_notebook_params(**kwargs):
    """
    A method for getting notebook params from local
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", f"daily_series_{kwargs['env']}.json")
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params = json.load(f)

    for channel, job_id in notebook_params["job_id"].items():
        ti.xcom_push(key=f"{channel}_job_id", value=job_id)

    ti.xcom_push(key="notebook_params", value=notebook_params)

def _get_total_period(**kwargs):
    """
    A method for getting total period
    """
    ti = kwargs["ti"]
    notebook_params = ti.xcom_pull(task_ids='get_notebook_params', key='notebook_params')

    scope = kwargs.pop("scope")
    s_date, e_date = DailyUtils.calc_total_period(notebook_params, scope, **kwargs)

    total_period = {
        "s_date": s_date,
        "e_date": e_date,
    }

    print(f"[CHECK-TOTAL-PERIOD]{total_period}")
    ti.xcom_push(key="total_period", value=total_period)

def _get_validation_args(**kwargs):
    """
    A method for setting arguments used for validation
    """
    
    # ================================================ (!) use fstring

    ti = kwargs["ti"]
    base_table_names = [
        "fb_series_ad_stats",
        # "gad_series_keyword_stat",
        # "gad_series_keyword_stat",
    ]

    print(f"[CHECK-VALIDATION-TABLE-NAME]{base_table_names}")
    ti.xcom_push(key="table_names", value=",".join(base_table_names))

check_validation_notebook_task = {
    "notebook_path": "/Shared/validation/check-data-validation",
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}
with DAG('autoreport_series_dag',
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["auto_report", "series", "all", "daily"]
    ) as dag:

    env = "dev"
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    get_total_period = PythonOperator(
        task_id="get_total_period",
        python_callable=_get_total_period,
        op_kwargs={"scope": "daily"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    with TaskGroup(group_id="databricks_run_job_group") as databricks_run_job_group:
        # @dongseok.lee
        # series_ad_api = DatabricksRunNowOperator(
        #     task_id="series_ad_api",
        #     job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='ad_job_id') }}",
        #     trigger_rule=TriggerRule.ALL_SUCCESS
        # )

        series_ad_stat_api = DatabricksRunNowOperator(
            task_id="series_ad_stat_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='ad_stat_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_gad_api = DatabricksRunNowOperator(
            task_id="series_gad_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='gad_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        with TaskGroup(group_id="series_fb_group") as series_fb_group:
            series_fb_api = DatabricksRunNowOperator(
                task_id="series_fb_api",
                job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='fb_job_id') }}",
                notebook_params={
                    "env": env
                },
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            series_fb_postprocess_api = DatabricksRunNowOperator(
                task_id="series_fb_postprocess_api",
                job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='fb_postprocess_job_id') }}",
                notebook_params={
                    "env": env
                },
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            series_fb_api >> series_fb_postprocess_api

        series_twt_api = DatabricksRunNowOperator(
            task_id="series_twt_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='twt_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_twt_org_api = DatabricksRunNowOperator(
            task_id="series_twt_org_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='twt_org_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_asa_api = DatabricksRunNowOperator(
            task_id="series_asa_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='asa_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_tik_api = DatabricksRunNowOperator(
            task_id="series_tik_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='tik_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_report_join_api = DatabricksRunNowOperator(
            task_id="series_report_join_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='report_join_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_report_stat_api = DatabricksRunNowOperator(
            task_id="series_report_stat_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='report_stat_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        series_ad_stat_api >> [series_gad_api, series_tik_api, series_twt_api, series_asa_api, series_fb_group, series_twt_org_api]
        [series_gad_api, series_tik_api, series_twt_api, series_asa_api, series_fb_group, series_twt_org_api] >> series_report_join_api >> series_report_stat_api

    get_validation_args = PythonOperator(
        task_id="get_validation_args",
        python_callable=_get_validation_args,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    check_validation = AutoReportValidationOperator(
        task_id="check_validation",
        notebook_task=check_validation_notebook_task,
        databricks_conn_id='databricks_default',
        existing_cluster_id="1026-083605-h88ik7f2",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
        table_names="{{ ti.xcom_pull(task_ids='get_validation_args', key='table_names') }}",
        project="AUTOREPORT" if env == "prod" else "AUTOREPORT_TEST"
    )

    start >> get_notebook_params >> get_total_period >> databricks_run_job_group >> get_validation_args >> check_validation >> end