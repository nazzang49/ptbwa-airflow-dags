import pprint
import logging

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