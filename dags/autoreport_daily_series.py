import os
import json
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pendulum.tz.timezone import Timezone
from custom.operators import AutoReportValidationOperator, DailyUtils, BaseUtils

_BASE_PATH = "/usr/local/airflow/dags"
def _get_notebook_params(**kwargs):
    """
    A method for getting notebook params
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", f"daily_series_{kwargs['env']}.json")
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params = json.load(f)

    for channel, job_id in notebook_params["job_id"]["api"].items():
        ti.xcom_push(key=f"{channel}_job_id", value=job_id)

    for channel, job_id in notebook_params["job_id"]["sql"].items():
        ti.xcom_push(key=f"{channel}_job_id", value=job_id)

    ti.xcom_push(key="api_job_ids", value=",".join([job_id for job_id in notebook_params["job_id"]["api"].values()]))
    ti.xcom_push(key="sql_job_ids", value=",".join([job_id for job_id in notebook_params["job_id"]["sql"].values()]))
    ti.xcom_push(key="notebook_params", value=notebook_params)

def _get_validation_config(**kwargs):
    """
    A method for getting data validation config
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", "databricks", "validation_config.json")
    with open(file_path, "r", encoding="utf-8") as f:
        validation_config = json.load(f)

    print(f"[CHECK-VALIDATION-CONFIG]{validation_config}")
    ti.xcom_push(key="validation_config", value=validation_config)

    # (!) changeable
    table_names = [
        "fb_series_ad_stats",
    ]

    print(f"[CHECK-VALIDATION-TABLE-NAME]{table_names}")
    ti.xcom_push(key="table_names", value=",".join(table_names))

def _get_update_jobs_config(**kwargs):
    """
    A method for getting update jobs config
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", "databricks", "update_jobs_config.json")
    with open(file_path, "r", encoding="utf-8") as f:
        update_jobs_config = json.load(f)

    update_jobs_config["base_parameters"] = {
        "env": kwargs["env"],
        "project": "autoreport",
        "advertisers": "series",
        "job_ids": ti.xcom_pull(task_ids='get_notebook_params', key=f'{kwargs["dag_type"]}_job_ids')
    }

    print(f"[CHECK-UPDATE-JOBS-CONFIG]{update_jobs_config}")
    ti.xcom_push(key="update_jobs_config", value=update_jobs_config)

def _check_data_interval(**kwargs):
    """
    A method for checking data interval
    desc:
        data_interval_end = now
    """
    converted_data_interval_end = BaseUtils.convert_pendulum_datetime_to_str(
        date=kwargs["data_interval_end"],
        format="%H:%M:%S",
        time_zone="Asia/Seoul"
    )

    if converted_data_interval_end == "17:40:00":
        return "get_notebook_params"
    else:
        return "trigger_sql_dag"

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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

################################### API ###################################


with DAG(f"{os.path.basename(__file__).replace('.py', '')}_api",
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval="*/10 16-18 * * *",
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["auto_report", "series", "all", "daily"]
    ) as dag_api:

    env = "prod"
    project = "autoreport"

    start = DummyOperator(task_id="start")

    check_data_interval = BranchPythonOperator(
        task_id="check_data_interval",
        python_callable=_check_data_interval,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    with TaskGroup(group_id="update_jobs_before_main_tasks") as update_jobs_before_main_tasks:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={
                "env": env,
                "dag_type": "api"
            },
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_before_main_tasks.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    get_total_period = PythonOperator(
        task_id="get_total_period",
        python_callable=_get_total_period,
        op_kwargs={"scope": "daily"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    with TaskGroup(group_id="main_tasks") as main_tasks:
        main_tasks_start = DummyOperator(
            task_id="main_tasks_start",
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

        series_fb_api = DatabricksRunNowOperator(
            task_id="series_fb_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='fb_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

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

        main_tasks_start >> [series_gad_api, series_tik_api, series_twt_api, series_asa_api, series_fb_api, series_twt_org_api]

    with TaskGroup(group_id="update_jobs_after_main_tasks") as update_jobs_after_main_tasks:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={
                "env": "prod",
                "dag_type": "api"
            },
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_after_main_tasks.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    with TaskGroup(group_id="validation_tasks") as validation_tasks:
        get_validation_config = PythonOperator(
            task_id="get_validation_config",
            python_callable=_get_validation_config,
            op_kwargs={"env": env},
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        check_validation = AutoReportValidationOperator(
            task_id="check_validation",
            notebook_task="{{ ti.xcom_pull(task_ids='validation_tasks.get_validation_config', key='validation_config') }}",
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
            table_names="{{ ti.xcom_pull(task_ids='validation_tasks.get_validation_config', key='table_names') }}",
            project="AUTOREPORT" if env == "prod" else "AUTOREPORT_TEST"
        )

        get_validation_config >> check_validation

    trigger_sql_dag = TriggerDagRunOperator(
        task_id="trigger_sql_dag",
        trigger_dag_id=f"{os.path.basename(__file__).replace('.py', '')}_sql",
        trigger_run_id=None,
        execution_date="{{ ts }}",
        reset_dag_run=False,
        wait_for_completion=False,
        # poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # trigger
    start >> check_data_interval >> [get_notebook_params, trigger_sql_dag]

    # common
    get_notebook_params >> get_total_period >> update_jobs_before_main_tasks >> main_tasks
    main_tasks >> update_jobs_after_main_tasks >> validation_tasks
    validation_tasks >> trigger_sql_dag >> end


################################### SQL ###################################


with DAG(f"{os.path.basename(__file__).replace('.py', '')}_sql",
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["auto_report", "series", "all", "daily"]
    ) as dag_sql:

    env = "prod"
    project = "autoreport"

    start = DummyOperator(task_id="start")
    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    with TaskGroup(group_id="update_jobs_before_main_tasks") as update_jobs_before_main_tasks:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={
                "env": env,
                "dag_type": "sql"
            },
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_before_main_tasks.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    get_total_period = PythonOperator(
        task_id="get_total_period",
        python_callable=_get_total_period,
        op_kwargs={"scope": "daily"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    with TaskGroup(group_id="main_tasks") as main_tasks:
        main_tasks_start = DummyOperator(
            task_id="main_tasks_start",
            trigger_rule=TriggerRule.NONE_FAILED
        )

        # @dongseok.lee
        series_ad_api = DatabricksRunNowOperator(
            task_id="series_ad_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='ad_job_id') }}",
            trigger_rule=TriggerRule.NONE_FAILED
        )

        series_ad_stat_sql = DatabricksRunNowOperator(
            task_id="series_ad_stat_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='ad_stat_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.NONE_FAILED
        )

        series_fb_postprocess_api = DatabricksRunNowOperator(
            task_id="series_fb_postprocess_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='fb_postprocess_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.NONE_FAILED
        )

        series_report_join_sql = DatabricksRunNowOperator(
            task_id="series_report_join_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='report_join_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.NONE_FAILED
        )

        series_report_stat_sql = DatabricksRunNowOperator(
            task_id="series_report_stat_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='report_stat_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.NONE_FAILED
        )

        main_tasks_start >> series_ad_api >> series_ad_stat_sql >> series_fb_postprocess_api >> series_report_join_sql >> series_report_stat_sql

    with TaskGroup(group_id="update_jobs_after_main_tasks") as update_jobs_after_main_tasks:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={
                "env": "prod",
                "dag_type": "sql"
            },
            trigger_rule=TriggerRule.NONE_SKIPPED,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.NONE_SKIPPED,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_after_main_tasks.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    with TaskGroup(group_id="validation_tasks") as validation_tasks:
        get_validation_config = PythonOperator(
            task_id="get_validation_config",
            python_callable=_get_validation_config,
            op_kwargs={"env": env},
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        check_validation = AutoReportValidationOperator(
            task_id="check_validation",
            notebook_task="{{ ti.xcom_pull(task_ids='validation_tasks.get_validation_config', key='validation_config') }}",
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            trigger_rule=TriggerRule.NONE_FAILED,
            total_period="{{ ti.xcom_pull(task_ids='get_total_period', key='total_period') }}",
            table_names="{{ ti.xcom_pull(task_ids='validation_tasks.get_validation_config', key='table_names') }}",
            project="AUTOREPORT" if env == "prod" else "AUTOREPORT_TEST"
        )

        get_validation_config >> check_validation

    # common
    start >> get_notebook_params >> get_total_period >> update_jobs_before_main_tasks >> main_tasks
    main_tasks >> update_jobs_after_main_tasks >> validation_tasks
    validation_tasks >> end