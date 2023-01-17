import os
import json

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pendulum.tz.timezone import Timezone
from custom.operators import AutoReportValidationOperator, DailyUtils

_BASE_PATH = "/usr/local/airflow/dags"
def _get_notebook_params(**kwargs):
    """
    A method for getting notebook params
    """
    ti = kwargs["ti"]
    file_path = os.path.join(_BASE_PATH, "configs", f"daily_pcar_{kwargs['env']}.json")
    with open(file_path, "r", encoding="utf-8") as f:
        notebook_params = json.load(f)

    job_ids = []
    for channel, job_id in notebook_params["job_id"].items():
        job_ids.append(job_id)
        ti.xcom_push(key=f"{channel}_job_id", value=job_id)

    ti.xcom_push(key="job_ids", value=",".join(job_ids))
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

    table_names = [
        "gad_pcar_adgroup_stats",
        "gad_pcar_asset_stats2",
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
        "advertisers": "pcar",
        "job_ids": ti.xcom_pull(task_ids='get_notebook_params', key='job_ids')
    }

    print(f"[CHECK-UPDATE-JOBS-CONFIG]{update_jobs_config}")
    ti.xcom_push(key="update_jobs_config", value=update_jobs_config)

def _check_env_before_main_tasks(**kwargs):
    """
    A method for checking env to branch
    """
    if kwargs["env"] == "dev":
        return "update_jobs_from_prod_to_dev.get_update_jobs_config"
    else:
        return "get_total_period"

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

def _check_env_after_main_tasks(**kwargs):
    """
    A method for checking env to branch
    """
    if kwargs["env"] == "dev":
        return "update_jobs_from_dev_to_prod.get_update_jobs_config"
    else:
        return "end"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}
with DAG(os.path.basename(__file__).replace(".py", ""),
    start_date=datetime(2022, 12, 19, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval="0 17 * * *",
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["auto_report", "pcar", "all", "daily"]
    ) as dag:

    env = "dev"
    project = "autoreport"

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_notebook_params = PythonOperator(
        task_id="get_notebook_params",
        python_callable=_get_notebook_params,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    check_env_before_main_tasks = BranchPythonOperator(
        task_id="check_env_before_main_tasks",
        python_callable=_check_env_before_main_tasks,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    with TaskGroup(group_id="update_jobs_from_prod_to_dev") as update_jobs_from_prod_to_dev:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={"env": env},
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_from_prod_to_dev.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    get_total_period = PythonOperator(
        task_id="get_total_period",
        python_callable=_get_total_period,
        op_kwargs={"scope": "daily"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    with TaskGroup(group_id="main_tasks") as main_tasks:
        pcar_af_api = DatabricksRunNowOperator(
            task_id="pcar_af_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='af_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_af_org_api = DatabricksRunNowOperator(
            task_id="pcar_af_org_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='af_org_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        # @yeonju.hong
        # pcar_af_stat_m_sql = DatabricksRunNowOperator(
        #     task_id="pcar_af_stat_m_sql",
        #     job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='af_stat_m_job_id') }}",
        #     notebook_params={
        #         "env": env
        #     },
        #     trigger_rule=TriggerRule.ALL_SUCCESS
        # )

        pcar_af_stat_d_sql = DatabricksRunNowOperator(
            task_id="pcar_af_stat_d_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='af_stat_d_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_af_stat_d_fb_sql = DatabricksRunNowOperator(
            task_id="pcar_af_stat_d_fb_sql",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='af_stat_d_fb_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_asa_api = DatabricksRunNowOperator(
            task_id="pcar_asa_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='asa_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_gad_api = DatabricksRunNowOperator(
            task_id="pcar_gad_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='gad_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_nsa_mo_api = DatabricksRunNowOperator(
            task_id="pcar_nsa_mo_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='nsa_mo_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_gs_api = DatabricksRunNowOperator(
            task_id="pcar_gs_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='gs_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        pcar_nsa_rent_api = DatabricksRunNowOperator(
            task_id="pcar_nsa_rent_api",
            job_id="{{ ti.xcom_pull(task_ids='get_notebook_params', key='nsa_rent_job_id') }}",
            notebook_params={
                "env": env
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        [pcar_nsa_mo_api, pcar_nsa_rent_api, pcar_af_api, pcar_gs_api, pcar_asa_api, pcar_gad_api, pcar_af_org_api] >> pcar_af_stat_d_sql >> pcar_af_stat_d_fb_sql

    check_env_after_main_tasks = BranchPythonOperator(
        task_id="check_env_after_main_tasks",
        python_callable=_check_env_after_main_tasks,
        op_kwargs={"env": env},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # (!) always triggered
    with TaskGroup(group_id="update_jobs_from_dev_to_prod") as update_jobs_from_dev_to_prod:
        get_update_jobs_config = PythonOperator(
            task_id="get_update_jobs_config",
            python_callable=_get_update_jobs_config,
            op_kwargs={"env": "prod"},
            trigger_rule=TriggerRule.NONE_SKIPPED,
        )

        update_jobs = DatabricksSubmitRunOperator(
            task_id="update_jobs",
            trigger_rule=TriggerRule.NONE_SKIPPED,
            databricks_conn_id='databricks_default',
            existing_cluster_id="1026-083605-h88ik7f2",
            notebook_task="{{ ti.xcom_pull(task_ids='update_jobs_from_dev_to_prod.get_update_jobs_config', key='update_jobs_config') }}"
        )

        get_update_jobs_config >> update_jobs

    # @jinyoung.park
    # update_jobs = DatabricksSubmitRunOperator(
    #     task_id="update_jobs",
    #     trigger_rule=TriggerRule.ALL_SUCCESS,
    #     databricks_conn_id='databricks_default',
    #     existing_cluster_id="1026-083605-h88ik7f2",
    #     notebook_task="{{ ti.xcom_pull(task_ids='get_update_jobs_config', key='update_jobs_config') }}"
    # )

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

    # dev
    update_jobs_from_prod_to_dev >> main_tasks
    update_jobs_from_dev_to_prod >> validation_tasks

    # common
    start >> get_notebook_params >> get_total_period >> check_env_before_main_tasks >> [update_jobs_from_prod_to_dev, main_tasks]
    main_tasks >> check_env_after_main_tasks >> [update_jobs_from_dev_to_prod, validation_tasks]
    validation_tasks >> end