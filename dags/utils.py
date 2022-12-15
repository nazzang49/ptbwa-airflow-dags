import os
import pprint
import logging
import json
import math

from airflow import AirflowException

class DagUtils:
    """
    A class for providing reused-functions especially in auto-report
    """
    _BASE_PATH = "/usr/local/airflow/dags/"

    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def get_intervals_len(adv, channel, env):
        """
        A method for getting intervals length
        """
        config = DagUtils.get_config(adv, channel, env)
        washing_preset = config["washing_preset"]
        washing_period = config["washing_period"]
        washing_interval = config["washing_interval"]

        if washing_preset:
            if washing_preset == "last_month":
                diff = 30
            elif washing_preset == "last_week":
                diff = 7
            else:
                raise AirflowException("[INVALID-VALUE]WASHING-PRESET-CANDIDATES::[last_month, last_week]")
        else:
            s_minus, e_minus = washing_period.split("|")
            diff = int(s_minus) - int(e_minus)

        washing_interval = diff if washing_interval >= diff else washing_interval
        intervals_len = math.ceil(diff / washing_interval)  # min = 1
        return intervals_len, washing_interval

    @staticmethod
    def get_config(adv, channel, env, is_prefix=True):
        """
        A method for getting config file path

        :param adv: e.g. kcar
        :param channel: e.g. kmt
        :param env: dev | prod
        :return:
        """
        config_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = DagUtils._BASE_PATH if is_prefix else config_dir

        config_name = f"{adv}_{channel}_{env}.json"
        config_path = os.path.join(base_path, "configs", config_name)

        with open(config_path, "r", encoding="utf-8") as f:
            notebook_params = json.load(f)

        print(f"[CHECK-CONFIG(INTERVALS_LEN)]{notebook_params}")
        return notebook_params

class PtbwaUtils:
    """
    A class for providing reused-functions
    """
    @property
    def log(self) -> logging.Logger:
        try:
            return self._log
        except AttributeError:
            self._log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._log

    def check_result(self, domain, result):
        try:
            print(f"[{domain}-RESULT]")
            pprint.pprint(result)
            print()
            self.log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e