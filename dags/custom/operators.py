from __future__ import annotations

import json
import pendulum
import pandas as pd

from datetime import datetime, timedelta
from typing import Sequence, Any
from enum import Enum

from pytz import *
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.compat.functools import cached_property
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksJobRunLink, _handle_databricks_operator_execution
from airflow.providers.databricks.utils.databricks import normalise_json_content

class AutoReportTypes(Enum):
    """
    An enum class for providing types to custom classes
    """

    BASE_PATH = "/usr/local/airflow/dags/"
    LAST_MONTH = "last_month"
    LAST_WEEK = "last_week"

    SCOPES = [
        "daily",
        "washing"
    ]

    WASHING_PRESETS = [
        "last_month",
        "last_week"
    ]

    NOTEBOOK_PARAMS_KEYS = [
        "washing_params",
        "interval_s",
        "interval_e",
        "data_interval_start",
        "data_interval_end"
    ]

    BASE_PARAMS_KEYS = [
        "base_parameters",
        "notebook_path",
    ]

class Validator:
    """
    A decorator class for validating argument
        checkpoint
        1. pre-validation
        2. post-validation

        check on
        1. empty
        2. (optional) candidates    e.g. scope
        3. (optional) required      e.g. notebook_params
    """

    _CANDIDATES = {
        "set_total_period": AutoReportTypes.SCOPES.value,
    }

    _REQUIRED = {
        "set_notebook_params": AutoReportTypes.NOTEBOOK_PARAMS_KEYS.value,
        "set_base_params": AutoReportTypes.BASE_PARAMS_KEYS.value,
    }

    def __init__(self, do_pre_validation: bool, do_post_validation: bool) -> None:
        self.do_pre_validation = do_pre_validation
        self.do_post_validation = do_post_validation

    def __call__(self, function):

        def inner(*args, **kwargs):
            method_name = function.__name__
            if self.do_pre_validation:
                Validator._pre_validation(args, method_name)

            result = function(*args, **kwargs)
            print(f"[VALIDATION-RESULT]{result}")

            if self.do_post_validation:
                Validator._post_validation(args, method_name)
            return result

        return inner

    @staticmethod
    def _pre_validation(args, method_name):
        """
        A method for validating before running method

        :param args:
        :param method_name:
        :return:
        """

        for arg in args:
            if not isinstance(arg, dict) and method_name in Validator._CANDIDATES:
                Validator._is_empty(arg, method_name)
                Validator._is_candidate(arg, method_name)

    @staticmethod
    def _post_validation(args, method_name):
        """
        A method for validating after running method

        :param args:
        :param method_name:
        :return:
        """
        for arg in args:
            if isinstance(arg, dict) and method_name in Validator._REQUIRED:
                Validator._is_required(arg, method_name)
                Validator._is_valid_type(arg, method_name, str)

    @staticmethod
    def _is_empty(arg, method_name):
        if not arg:
            raise AirflowException(
                f"[REQUIRED-ARG]METHOD::{method_name}"
            )

    @staticmethod
    def _is_candidate(arg, method_name):
        if arg not in Validator._CANDIDATES[method_name]:
            raise AirflowException(
                f"[NOT-FOUND-KEY(CANDIDATE)]METHOD::{method_name}|ARG::{arg}|CANDIDATES::{Validator._CANDIDATES[method_name]}"
            )

    @staticmethod
    def _is_required(arg: dict, method_name):
        for req_key in Validator._REQUIRED[method_name]:
            if req_key not in arg:
                raise AirflowException(
                    f"[NOT-FOUND-KEY(REQUIRED)]METHOD::{method_name}|ARG::{arg}|CANDIDATES::{Validator._REQUIRED[method_name]}"
                )

    @staticmethod
    def _is_valid_type(arg: dict, method_name, value_type=None):
        for key in arg.keys():
            if isinstance(arg[key], dict):
                Validator._is_valid_type(arg[key], method_name, value_type)
            elif not isinstance(arg[key], value_type):
                raise AirflowException(
                    f"[INVALID-TYPE]METHOD::{method_name}|ARG::{arg}|CANDIDATES::{Validator._REQUIRED[method_name]}|VALUE-TYPE::{value_type}"
                )

class AutoReportWashOperator(BaseOperator):
    """
    A class for washing previous data
    """
    template_fields: Sequence[str] = ('json', 'databricks_conn_id')
    template_ext: Sequence[str] = ('.json-tpl',)
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        job_id: str | None = None,
        job_name: str | None = None,
        json: Any | None = None,
        notebook_params: dict[str, str] | None = None,
        python_params: list[str] | None = None,
        jar_params: list[str] | None = None,
        spark_submit_params: list[str] | None = None,
        python_named_params: dict[str, str] | None = None,
        idempotency_token: str | None = None,
        databricks_conn_id: str = 'databricks_default',
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        do_xcom_push: bool = True,
        wait_for_termination: bool = True,
        interval_idx: int | None = None,
        interval_freq: str | None = None,
        total_period: dict[str, str] | None = None,
        is_interval: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination

        if job_id is not None:
            self.json['job_id'] = job_id
        if job_name is not None:
            self.json['job_name'] = job_name
        if 'job_id' in self.json and 'job_name' in self.json:
            raise AirflowException("Argument 'job_name' is not allowed with argument 'job_id'")
        if notebook_params is not None:
            self.json['notebook_params'] = notebook_params
        if python_params is not None:
            self.json['python_params'] = python_params
        if python_named_params is not None:
            self.json['python_named_params'] = python_named_params
        if jar_params is not None:
            self.json['jar_params'] = jar_params
        if spark_submit_params is not None:
            self.json['spark_submit_params'] = spark_submit_params
        if idempotency_token is not None:
            self.json['idempotency_token'] = idempotency_token

        # ========================== CUSTOM PARAMS ==========================

        if notebook_params is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{notebook_params}")
        if total_period is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{total_period}")

        if interval_idx is None:
            print("[NOT-FOUND-INTERVAL-IDX]THIS-IS-TOTAL-INTERVAL")
        if interval_freq is None:
            print("[NOT-FOUND-INTERVAL-FREQ]THIS-IS-TOTAL-INTERVAL")

        self.interval_idx = interval_idx
        self.interval_freq = interval_freq
        self.is_interval = is_interval
        self.json["total_period"] = total_period

        self.json = normalise_json_content(self.json)
        self.run_id: int | None = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="AutoReportWashOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        """
        A method for executing databricks run-now with custom logic
        """
        hook = self._hook
        if 'job_name' in self.json:
            job_id = hook.find_job_id_by_name(self.json['job_name'])
            if job_id is None:
                raise AirflowException(f"Job ID for job name {self.json['job_name']} can not be found")
            self.json['job_id'] = job_id
            del self.json['job_name']

        notebook_params = self.json["notebook_params"]
        total_period = self.json["total_period"]
        total_period["s_date"] = pd.Timestamp(total_period["s_date"])
        total_period["e_date"] = pd.Timestamp(total_period["e_date"])

        if self.is_interval:
            interval_groups = self.set_intervals(total_period)
            interval_s = interval_groups[self.interval_idx][0]
            interval_e = interval_groups[self.interval_idx][1]

        self.set_notebook_params(
            interval_s if self.is_interval else total_period["s_date"],
            interval_e if self.is_interval else total_period["e_date"],
            notebook_params,
            **context
        )

        self.json.pop("total_period")   # total_period is not serializable in json
        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def set_intervals(self, total_period: dict):
        intervals = pd.date_range(
            start=total_period["s_date"],
            end=total_period["e_date"],
            freq=self.interval_freq
        )
        intervals = list(intervals)
        n_interval_freq = int(self.interval_freq[0])


        # ======================== use generator ========================


        # last_interval = Utils.convert_pandas_timestamp_to_str(intervals[-1])
        # if total_period["e_date"] == last_interval:
        #     intervals = intervals[:-1]  # delete last interval

        interval_groups = [
            [interval, interval + timedelta(days=n_interval_freq)] for interval in intervals
        ]
        interval_groups[-1][1] = total_period["e_date"]  # last group
        return interval_groups

    @Validator(do_pre_validation=False, do_post_validation=True)
    def set_notebook_params(self, interval_s: pd.Timestamp, interval_e: pd.Timestamp, notebook_params: dict, **kwargs):
        """
        A method for setting notebook params as string-value
        """
        washing_params = {
            k: v for k, v in notebook_params.items() if k.startswith(("washing_", "notebook_name"))
        }

        notebook_params.clear()
        notebook_params["washing_params"] = json.dumps(washing_params)
        notebook_params["interval_s"] = Utils.convert_pandas_timestamp_to_str(interval_s)
        notebook_params["interval_e"] = Utils.convert_pandas_timestamp_to_str(interval_e)

        notebook_params["data_interval_start"] = Utils.convert_pendulum_datetime_to_str(
            kwargs["data_interval_start"],
            time_zone="Asia/Seoul"
        )
        notebook_params["data_interval_end"] = Utils.convert_pendulum_datetime_to_str(
            kwargs["data_interval_end"],
            time_zone="Asia/Seoul"
        )

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                'Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id
            )
        else:
            self.log.error(
                'Error: Task: %s with invalid run_id was requested to be cancelled.', self.task_id
            )

class AutoReportValidationOperator(BaseOperator):
    """
    A class for validating data or result after databricks-api calls
    """
    template_fields: Sequence[str] = ('json', 'databricks_conn_id')
    template_ext: Sequence[str] = ('.json-tpl',)
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        json: Any | None = None,
        tasks: list[object] | None = None,
        spark_jar_task: dict[str, str] | None = None,
        notebook_task: dict[str, str] | None = None,
        spark_python_task: dict[str, str | list[str]] | None = None,
        spark_submit_task: dict[str, list[str]] | None = None,
        pipeline_task: dict[str, str] | None = None,
        dbt_task: dict[str, str | list[str]] | None = None,
        new_cluster: dict[str, object] | None = None,
        existing_cluster_id: str | None = None,
        libraries: list[dict[str, str]] | None = None,
        run_name: str | None = None,
        timeout_seconds: int | None = None,
        databricks_conn_id: str = 'databricks_default',
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        do_xcom_push: bool = True,
        idempotency_token: str | None = None,
        access_control_list: list[dict[str, str]] | None = None,
        wait_for_termination: bool = True,
        git_source: dict[str, str] | None = None,
        total_period: dict[str, str] | None = None,
        table_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination

        if tasks is not None:
            self.json['tasks'] = tasks
        if spark_jar_task is not None:
            self.json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
        if spark_python_task is not None:
            self.json['spark_python_task'] = spark_python_task
        if spark_submit_task is not None:
            self.json['spark_submit_task'] = spark_submit_task
        if pipeline_task is not None:
            self.json['pipeline_task'] = pipeline_task
        if dbt_task is not None:
            self.json['dbt_task'] = dbt_task
        if new_cluster is not None:
            self.json['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            self.json['existing_cluster_id'] = existing_cluster_id
        if libraries is not None:
            self.json['libraries'] = libraries
        if run_name is not None:
            self.json['run_name'] = run_name
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs['task_id']
        if idempotency_token is not None:
            self.json['idempotency_token'] = idempotency_token
        if access_control_list is not None:
            self.json['access_control_list'] = access_control_list
        if git_source is not None:
            self.json['git_source'] = git_source
        if 'dbt_task' in self.json and 'git_source' not in self.json:
            raise AirflowException('git_source is required for dbt_task')

        # ========================== CUSTOM PARAMS ==========================

        if total_period is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{total_period}")
        if table_name is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{table_name}")

        self.json["total_period"] = total_period
        self.json["table_name"] = table_name

        self.json = normalise_json_content(self.json)
        self.run_id: int | None = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="AutoReportValidationOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        """
        A method for executing databricks run-now for data validation
        """
        notebook_task = self.json["notebook_task"]
        self.set_base_params(notebook_task, **context)

        self.run_id = self._hook.submit_run(self.json)
        _handle_databricks_operator_execution(self, self._hook, self.log, context)

    @Validator(do_pre_validation=False, do_post_validation=True)
    def set_base_params(self, notebook_task: dict, **kwargs):
        notebook_task["base_parameters"] = {
            "s_date": self.json["total_period"]["s_date"],
            "e_date": self.json["total_period"]["e_date"],
            "table_name": self.json["table_name"],
            "data_interval_start": Utils.convert_pendulum_datetime_to_str(
                kwargs["data_interval_start"],
                time_zone="Asia/Seoul"
            ),
            "data_interval_end": Utils.convert_pendulum_datetime_to_str(
                kwargs["data_interval_end"],
                time_zone="Asia/Seoul"
            ),
        }

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                'Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id
            )
        else:
            self.log.error(
                'Error: Task: %s with invalid run_id was requested to be cancelled.', self.task_id
            )


class Utils:
    """
    A class for providing reused-functions especially in custom-operators
    """
    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    @Validator(do_pre_validation=True, do_post_validation=False)
    def calc_total_period(notebook_params: dict, scope: str, **kwargs) -> [str, str]:
        """
        A method for setting whole intervals
        :return:
        """
        current_date = Utils.convert_pendulum_datetime_to_str(kwargs["data_interval_end"])
        current_date = Utils.convert_str_to_datetime(current_date[:10])

        print("notebook-params : ", notebook_params)

        washing_preset = notebook_params["washing_preset"]
        washing_period = notebook_params["washing_period"]

        if washing_preset:
            if washing_preset == AutoReportTypes.LAST_MONTH.value:
                s_date = Utils.get_first_day_of_last_month(current_date)
                e_date = Utils.get_first_day_of_this_month(current_date)
            elif washing_preset == AutoReportTypes.LAST_WEEK.value:
                s_date = Utils.get_first_day_of_last_week(current_date)
                e_date = Utils.get_first_day_of_this_week(current_date)
            else:
                raise AirflowException(
                    f"[INVALID-VALUE]WASHING-PRESET-CANDIDATES::{AutoReportTypes.WASHING_PRESETS.value}|SCOPES::{scope}"
                )
        else:
            s_minus, e_minus = washing_period.split("|")
            s_date = Utils.get_date_of_period(current_date, int(s_minus))
            e_date = Utils.get_date_of_period(current_date, int(e_minus))

        # (!) datetime is not serializable in xcom_push
        s_date = Utils.convert_default_datetime_to_str(s_date)
        e_date = Utils.convert_default_datetime_to_str(e_date)

        return s_date, e_date

    @staticmethod
    def get_date_of_period(current_date: datetime, d_minus: int) -> datetime:
        return current_date - timedelta(days=d_minus)

    @staticmethod
    def get_first_day_of_last_week(current_date: datetime) -> datetime:
        return current_date - timedelta(days=current_date.weekday(), weeks=1)

    @staticmethod
    def get_first_day_of_this_week(current_date: datetime) -> datetime:
        return current_date - timedelta(days=current_date.weekday(), weeks=0)

    @staticmethod
    def get_first_day_of_this_month(current_date: datetime) -> datetime:
        return datetime(year=current_date.year, month=current_date.month, day=1)

    @staticmethod
    def get_first_day_of_last_month(current_date: datetime) -> datetime:
        return datetime(year=current_date.year, month=current_date.month - 1, day=1)

    @staticmethod
    def convert_default_datetime_to_str(date: datetime, format="%Y-%m-%d", time_zone=None):
        return date.astimezone(timezone(time_zone)).strftime(format) if time_zone else date.strftime(format)

    @staticmethod
    def convert_pandas_timestamp_to_str(date: pd.Timestamp, format="%Y-%m-%d", time_zone=None):
        return date.tz_convert(time_zone).strftime(format) if time_zone else date.strftime(format)

    @staticmethod
    def convert_pendulum_datetime_to_str(date: pendulum.datetime, format="%Y-%m-%d %H:%M:%S", time_zone=None):
        return date.in_timezone(time_zone).strftime(format) if time_zone else date.strftime(format)

    @staticmethod
    def convert_str_to_datetime(date: str, format="%Y-%m-%d"):
        return datetime.strptime(date, format)