import logging
import time
from typing import Set, List

from boto3.dynamodb.conditions import Attr

from figgy.constants.data import *
from figgy.data.models.config_item import ConfigItem

log = logging.getLogger(__name__)


class ConfigDao:
    """
    This DAO executes queries against various DynamoDB tables required for our config management system's operation.
    2) Audit Table
    3) Cache table.
    """

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._cache_table = self._dynamo_resource.Table(CACHE_TABLE_NAME)

    def get_all_config_names(self, prefix: str = None,
                             exclude_prefixes=None,
                             one_level: bool = False,
                             start_key: str = None) -> Set[str]:
        """
        Retrieve all key names from the Dynamo DB config-cache table in each account. Much more efficient than
        querying SSM directly.
        Args:
            start_key: Optional: Is used for recursive paginated lookups to get the full data set. This should not
            be passed in by the user.
            exclude_prefixes: configs with these prefixes will be excluded from results
        Returns:

        """

        if exclude_prefixes is None:
            exclude_prefixes = ['/figgy']

        start_time = time.time()
        if start_key:
            log.info(f"Recursively scanning with start key: {start_key}")
            result = self._cache_table.scan(ExclusiveStartKey=start_key)
        else:
            result = self._cache_table.scan()

        configs: Set[str] = set()
        if "Items" in result and len(result['Items']) > 0:
            for item in result['Items']:
                name = item[CACHE_PARAMETER_KEY_NAME]
                configs.add(name)

        if prefix:
            configs = set(filter(lambda x: x.startswith(prefix), configs))

        if exclude_prefixes:
            for excluded_prefix in exclude_prefixes:
                configs = set(filter(lambda x: not x.name.startswith(excluded_prefix), configs))

        if one_level:
            configs = set(filter(lambda x: len(x.split('/')) == len(prefix.split('/')) + 1, configs))

        if 'LastEvaluatedKey' in result:
            configs = configs | self.get_all_config_names(prefix, exclude_prefix, one_level,
                                                          start_key=result['LastEvaluatedKey'])

        log.info(
            f"Returning config names from dynamo cache after: {time.time() - start_time} "
            f"seconds with {len(configs)} configs.")
        return configs

    def get_config_names_after(self, millis_since_epoch: int, exclude_prefixes=None) -> Set[ConfigItem]:
        """
        Retrieve all key names from the Dynamo DB config-cache table in each account. Much more efficient than
        querying SSM directly.
        Args:
            millis_since_epoch: milliseconds in epoch to lookup config names from cache after
            exclude_prefixes: configs with these prefixes will be excluded from results
        Returns: Set[str] -> configs that have been added to cache table after millis_since_epoch
        """

        if exclude_prefixes is None:
            exclude_prefixes = ['/figgy']

        request = {
            'attribute_names': {
                '#n1': CACHE_LAST_UPDATED_KEY_NAME
            },
            'attribute_values': {
                ':v1': {'N': millis_since_epoch}
            },
            'expression': Attr(CACHE_LAST_UPDATED_KEY_NAME).gt(millis_since_epoch)
        }

        start_time = time.time()
        response = self._cache_table.scan(
            FilterExpression=request['expression']
        )

        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = self._cache_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items = items + response.get('Items', [])

        configs: Set[ConfigItem] = set()

        for item in items:
            configs.add(ConfigItem(**item))

        if exclude_prefixes:
            for excluded_prefix in exclude_prefixes:
                configs = set(filter(lambda x: not x.name.startswith(excluded_prefix), configs))

        log.info(f"Returning {len(configs)} parameter names from dynamo cache after time: [{millis_since_epoch}] in "
                 f"{time.time() - start_time} seconds.")

        return configs

    def put_in_config_cache(self, name):
        item = {
            CACHE_PARAMETER_KEY_NAME: name
        }

        self._cache_table.put_item(Item=item)
