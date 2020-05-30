import logging
import sys


class LoggingUtils:

    @staticmethod
    def configure_logging(name: str, log_level=logging.INFO):
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        stdout_handler = logging.StreamHandler(sys.stdout)
        root_logger.addHandler(stdout_handler)

        log = logging.getLogger(name)
        log.setLevel(log_level)
        return log
