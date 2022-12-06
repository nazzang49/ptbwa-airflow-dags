from __future__ import annotations

import json

from logging import Logger
from datetime import datetime, timedelta
from typing import Sequence, Optional, Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.models.taskinstance import Context
from airflow.compat.functools import cached_property
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksJobRunLink, _handle_databricks_operator_execution
from airflow.providers.databricks.utils.databricks import normalise_json_content

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
        n_interval: int | None = None,
        d_interval: int | None = None,
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

        if n_interval is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{n_interval}")
        if d_interval is None:
            raise AirflowException("[NOT-FOUND]REQUIRED::{d_interval}")

        self.n_interval = n_interval    # i-th of whole interval groups
        self.d_interval = d_interval    # interval days e.g. 3 means auto increments by 3

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

        # self.set_date_params(context)
        self.set_periods(context)
        self.set_interval()
        self.set_notebook_params(context)

        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def set_periods(self, context: Context):
        """
        A method for creating date-range to do api call group by group
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

        self.s_date = s_date.strftime("%Y-%m-%d")
        self.e_date = e_date.strftime("%Y-%m-%d")
        self.periods = pd.date_range(start=self.s_date, end=self.e_date, freq=f"{self.d_interval}D").tolist()
        self.periods_len = len(self.periods)

    def set_interval(self):
        """
        A method for setting current interval in whole periods
        :return:
        """
        self.interval_s_date = self.periods[self.n_interval]
        if self.n_interval < self.periods_len:
            self.interval_e_date = self.periods[self.n_interval + 1]
        elif self.n_interval == self.periods_len:
            self.interval_e_date = self.e_date
        else:
            raise AirflowException(f"[OUT-OF-RANGE]INTERVAL::{self.n_interval}")

    def set_notebook_params(self, context: Context):
        """
        A method for setting notebook params as string-value
        :param context:
        :return:
        """
        self.json["notebook_params"]["washing_params"] = json.dumps(self.json["notebook_params"]["washing_params"])
        self.json["notebook_params"]["interval_s_date"] = self.interval_s_date.strftime("%Y-%m-%d")
        self.json["notebook_params"]["interval_e_date"] = self.interval_e_date.strftime("%Y-%m-%d")

        self.json["notebook_params"]["data_interval_start"] = \
            context["data_interval_start"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

        self.json["notebook_params"]["data_interval_end"] = \
            context["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

        self.json["notebook_params"]["execution_date"] = \
            context["execution_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

        self.json["notebook_params"]["next_execution_date"] = \
            context["next_execution_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

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