from __future__ import annotations

import json
import pendulum

from datetime import datetime, timedelta
from typing import Sequence, Any

from pytz import *
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.compat.functools import cached_property
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksJobRunLink, _handle_databricks_operator_execution
from airflow.providers.databricks.utils.databricks import normalise_json_content
from dags.utils import PtbwaUtils

# import AutoReportUtils

import pandas as pd

class AutoReportWashOperator(BaseOperator):

    template_fields: Sequence[str] = ('json', 'databricks_conn_id')
    template_ext: Sequence[str] = ('.json-tpl',)
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'
    operator_extra_links = (DatabricksJobRunLink(),)

    _WASHING_PRESETS = [
        "last_month",
        "last_week"
    ]

    _NOTEBOOK_PARAMS = [
        "washing_params",
        "interval_s_date",
        "interval_e_date",
        "data_interval_start",
        "data_interval_end",
        "execution_date",
        "next_execution_date"
    ]

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

        if interval_idx is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{n_interval}")
        if interval_freq is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{d_interval}")

        self.interval_idx = interval_idx          # i-th interval
        self.interval_freq = interval_freq        # e.g. 3D means [11-01, 11-04, 11-07]

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
        A method for executing custom logic
        :param context:
        :return:
        """
        hook = self._hook
        if 'job_name' in self.json:
            job_id = hook.find_job_id_by_name(self.json['job_name'])
            if job_id is None:
                raise AirflowException(f"Job ID for job name {self.json['job_name']} can not be found")
            self.json['job_id'] = job_id
            del self.json['job_name']

        self.set_intervals(context)
        self.set_current_interval()
        self.set_notebook_params(context)

        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def set_intervals(self, context: Context):
        """
        A method for creating intervals to do api call on each interval
        :return:
        """
        data_interval_end = context["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
        current_date = datetime.strptime(data_interval_end[:10], "%Y-%m-%d")
        washing_params = self.json["notebook_params"]["washing_params"]

        if washing_params["washing_preset"]:
            if washing_params["washing_preset"] == "last_month":
                s_date = datetime(year=current_date.year, month=current_date.month - 1, day=1)
                e_date = datetime(year=current_date.year, month=current_date.month, day=1)
            elif washing_params["washing_preset"] == "last_week":
                s_date = current_date - timedelta(days=current_date.weekday(), weeks=1)  # weeks=1 means "last_week_monday"
                e_date = current_date - timedelta(days=current_date.weekday(), weeks=0)  # weeks=0 means "this_week_monday"
            else:
                raise AirflowException(f"[INVALID-VALUE]APPLICABLE-WASHING-PRESET::{AutoReportWashOperator._WASHING_PRESETS}")
        else:
            s_minus, e_minus = washing_params["washing_period"].split("|")
            s_date = current_date - timedelta(days=int(s_minus))
            e_date = current_date - timedelta(days=int(e_minus))

        self.s_date = AutoReportUtils.convert_default_datetime_to_string(s_date)
        self.e_date = AutoReportUtils.convert_default_datetime_to_string(e_date)

        self.intervals = pd.date_range(start=self.s_date, end=self.e_date, freq=self.interval_freq).tolist()
        self.intervals_len = len(self.intervals)

    def set_current_interval(self):
        """
        A method for setting current interval in whole intervals
        :return:
        """
        washing_params = self.json["notebook_params"]["washing_params"]

        self.interval_s = self.intervals[self.interval_idx]
        if self.interval_idx < self.intervals_len:
            self.interval_e = self.intervals[self.interval_idx + 1]
        elif self.interval_idx == self.intervals_len:
            self.interval_e = self.e_date
            if washing_params["washing_preset"] == "last_week":
                self.interval_e += timedelta(days=1)
        else:
            raise AirflowException(f"[OUT-OF-RANGE]INTERVAL::{self.interval_idx}")

    def set_notebook_params(self, context: Context):
        """
        A method for setting notebook params as string-value
        :param context:
        :return:
        """
        self.json["notebook_params"]["washing_params"] = json.dumps(self.json["notebook_params"]["washing_params"])
        self.json["notebook_params"]["interval_s_date"] = AutoReportUtils.convert_pandas_timestamp_to_string(self.interval_s)
        self.json["notebook_params"]["interval_e_date"] = AutoReportUtils.convert_pandas_timestamp_to_string(self.interval_e)

        self.json["notebook_params"]["dis"] = AutoReportUtils.convert_pendulum_datetime_to_string(context["data_interval_start"], time_zone="Asia/Seoul")
        self.json["notebook_params"]["die"] = AutoReportUtils.convert_pendulum_datetime_to_string(context["data_interval_end"], time_zone="Asia/Seoul")

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

class AutoReportUtils:
    """
    A class for providing reused-functions
    """
    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def convert_default_datetime_to_string(date: datetime, format="%Y-%m-%d", time_zone=None):
        return date.astimezone(timezone(time_zone)).strftime(format) if time_zone else date.strftime(format)

    @staticmethod
    def convert_pandas_timestamp_to_string(date: pd.Timestamp, format="%Y-%m-%d", time_zone=None):
        return date.tz_convert(time_zone).strftime(format) if time_zone else date.strftime(format)

    @staticmethod
    def convert_pendulum_datetime_to_string(date:pendulum.datetime, format="%Y-%m-%d %H:%M:%S", time_zone=None):
        return date.in_timezone(time_zone).strftime(format) if time_zone else date.strftime(format)


