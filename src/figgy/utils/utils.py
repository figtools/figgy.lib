import logging
import re
import time
from typing import List, Any

import botocore
import urllib3

log = logging.getLogger(__name__)
BACKOFF = .25
MAX_RETRIES = 10


class Utils:

    @staticmethod
    def parse_namespace(app_key: str) -> str:
        ns = None
        try:
            get_ns = re.compile(r"^(/app/[A-Za-z0-9_-]+/).*")
            val = get_ns.match(app_key)
            ns = val.group(1)
        except (AttributeError, TypeError) as e:
            print(f"Unable to parse namespace from {app_key}. If your app_parameters block values do not begin with "
                  f"the prefix /app/your-service-name , you must include the 'namespace' property in your figgy.json "
                  f"with value /app/your-service-name/")

        return ns

    @staticmethod
    def retry(function):
        """
        Decorator that supports automatic retries if connectivity issues are detected with boto or urllib operations
        """

        def inner(self, *args, **kwargs):
            retries = 0
            while True:
                try:
                    return function(self, *args, **kwargs)
                except (botocore.exceptions.EndpointConnectionError, urllib3.exceptions.NewConnectionError) as e:
                    if retries > MAX_RETRIES:
                        raise e

                    self._utils.notify("Network connectivity issues detected. Retrying with back off...")
                    retries += 1
                    time.sleep(retries * BACKOFF)

        return inner

    @staticmethod
    def trace(func):
        """
        Decorator that adds logging around function execution and function parameters.
        """

        def wrapper(*args, **kwargs):
            log.info(f"Entering function: {func.__name__} with args: {args}")
            start = time.time()
            result = func(*args, **kwargs)
            log.info(f"Exiting function: {func.__name__} and returning: {result}")
            log.info(f"Function complete after {round(time.time() - start, 2)} seconds.")
            return result

        return wrapper

    @staticmethod
    def chunk_list(lst: List, chunk_size: int) -> List[List]:
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    @staticmethod
    def validate(boolean: bool, error_msg: str):
        if not boolean:
            raise ValueError(error_msg)

    @staticmethod
    def millis_since_epoch():
        return int(time.time() * 1000)

    @staticmethod
    def validate_set(obj: Any, obj_name: str):
        if not obj:
            raise ValueError(f'{obj_name} was not set and expected.')
