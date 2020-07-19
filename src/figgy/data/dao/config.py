from boto3.dynamodb.conditions import Key, Attr
from decimal import *
from figgy.constants.data import *
from figgy.data.models.config_item import ConfigItem
from figgy.models.replication_config import ReplicationConfig, ReplicationType
from figgy.models.restore_config import RestoreConfig
from figgy.models.run_env import RunEnv
from figgy.models.audit_log import AuditLog
from figgy.models.parameter_history import ParameterHistory
from figgy.models.parameter_store_history import PSHistory
from typing import Callable, Dict, List, Any, Set
import logging
import datetime
import time

from figgy.utils.utils import Utils

log = logging.getLogger(__name__)


class ConfigDao:
    """
    This DAO executes queries against various DynamoDB tables required for our config management system's operation.
    1) Config replication table
    2) Audit Table
    3) Cache table.
    """

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._config_repl_table = self._dynamo_resource.Table(REPL_TABLE_NAME)
        self._audit_table = self._dynamo_resource.Table(AUDIT_TABLE_NAME)
        self._cache_table = self._dynamo_resource.Table(CACHE_TABLE_NAME)

    def delete_config(self, destination: str) -> None:
        """
        Deletes a Replication configuration from the DB
        Args:
            destination: str -> /path/to/configuration/destination
        """
        self._config_repl_table.delete_item(
            Key={REPL_DEST_KEY_NAME: destination}
        )

    def get_parameter_restore_details(self, ps_name: str, start_key: str = None) -> List[RestoreConfig]:
        """
        :param ps_name:  str -> parameter store key name
        :param start_key: str -> used for recursive lookups on table scans.
        :return:
            List of parameter name + value + description + type
        """
        results = []
        filter_exp = Key(AUDIT_PARAMETER_KEY_NAME).eq(ps_name)
        if start_key:
            result = self._audit_table.scan(FilterExpression=filter_exp, ExclusiveStartKey=start_key)
        else:
            result = self._audit_table.scan(FilterExpression=filter_exp)

        items: List = result["Items"] if result["Items"] else []

        # Remove items from list where action != "PutParameter"
        items = list(filter(lambda x: x["action"] == "PutParameter", items))

        results = sorted(RestoreConfig.convert_to_model(items), key=lambda x: x.ps_time, reverse=True)

        if 'LastEvaluatedKey' in result:
            results = results + self.get_parameter_restore_details(ps_name, start_key=result['LastEvaluatedKey'])

        # Convert to RestoreConfig model then sort list chronologically by timestamp
        return results

    def get_all_parameter_history(self, ps_time: datetime.datetime, ps_prefix: str) -> List[RestoreConfig]:
        """
        Scans in DynamoDb only do 1MB at at time. For large tables, we need to tell dynamo to KEEP_SCANNING. After each
        result set is returned, we need to inform dynamo to keep scanning to find ALL results across the full table. This
        will continue going back to dynamo until we find the _full_ history of a parameter.
        Args:
            ps_time: Time up to which parameter history should be returned.
            ps_prefix: e.g. /shared/some/prefix - Prefix to query under
            start_key: LastEvaluatedKey returned in scan results. Lets you konw if there is more scanning that can be done.
        Returns: List[RestoreConfig] mapped from histories
        """
        time_end = Decimal(ps_time.timestamp() * 1000)
        filter_exp = Key(AUDIT_TIME_KEY_NAME).lt(time_end) & Key(AUDIT_PARAMETER_KEY_NAME).begins_with(ps_prefix)

        result = self._audit_table.scan(FilterExpression=filter_exp)
        items = result.get('Items', [])

        while 'LastEvaluatedKey' in result:
            result = self._audit_table.scan(FilterExpression=filter_exp, ExclusiveStartKey=result['LastEvaluatedKey'])
            items = items + result.get('Items', [])

        return RestoreConfig.convert_to_model(items)

    def get_parameter_history_before_time(self, ps_time: datetime.datetime, ps_prefix: str) -> PSHistory:
        """
        Retrieves total parameter history for all parameters up until the datetime passed in under the provided prefix.
        Args:
            ps_time: Time up to which parameter history should be returned.
            ps_prefix: e.g. /shared/some/prefix - Prefix to query under

        Returns:

        """

        restore_cfgs: List[RestoreConfig] = self.get_all_parameter_history(ps_time, ps_prefix)

        ps_histories: Dict[str, ParameterHistory] = {}
        for cfg in restore_cfgs:
            if ps_histories.get(cfg.ps_name, None):
                ps_histories.get(cfg.ps_name).add(cfg)
            else:
                ps_histories[cfg.ps_name] = ParameterHistory.instance(cfg)

        return PSHistory(list(ps_histories.values()))

    def get_parameter_restore_range(self, ps_time: datetime.datetime, ps_prefix: str) \
            -> List[RestoreConfig]:
        """
        :param ps_time:  int -> datetime to query dynamo timestamp from
        :param ps_prefix: str -> parameter store prefix we will recursively restore from/to (e.g., /app/demo-time)
        :return:
            List of parameter name + value + description + type for given time range
        """
        time_end = Decimal(ps_time.timestamp() * 1000)

        filter_exp = Key(AUDIT_TIME_KEY_NAME).lt(time_end) & Key(AUDIT_PARAMETER_KEY_NAME).begins_with(ps_prefix) \
                     & Attr(AUDIT_ACTION_ATTR_NAME).eq(SSM_PUT)

        result = self._audit_table.scan(FilterExpression=filter_exp)
        items = list(filter(lambda x: x["action"] == "PutParameter", result.get('Items', [])))

        while 'LastEvaluatedKey' in result:
            result = self._audit_table.scan(FilterExpression=filter_exp, ExclusiveStartKey=result['LastEvaluatedKey'])
            items = items + list(filter(lambda x: x["action"] == "PutParameter", result.get('Items', [])))

        return sorted(RestoreConfig.convert_to_model(items), key=lambda x: x.ps_time, reverse=True)



    def get_all_configs(self, namespace: str, start_key: str = None) -> List[ReplicationConfig]:
        """
        Retrieves all replication configs from the database for a particular namespace
        Args:
            start_key: LastEvaluatedKey returned in scan results. Lets you konw if there is more scanning that can be done.
            namespace: namespace  - e.g. /app/demo-time/

        Returns:
            List of ReplicationConfigs that match the namespace.

        """
        filter_exp = Attr(REPL_NAMESPACE_ATTR_NAME).eq(namespace)

        result = self._config_repl_table.scan(FilterExpression=filter_exp)
        configs = [self.__cfg_from_boto_item(item) for item in result.get('Items', [])]

        while 'LastEvaluatedKey' in result:
            result = self._config_repl_table.scan(FilterExpression=filter_exp, ExclusiveStartKey=start_key)
            configs = configs + [self.__cfg_from_boto_item(item) for item in result.get('Items', [])]

        return configs

    def get_cfgs_by_src(self, source: str) -> List[ReplicationConfig]:
        """
        Args:
            source: Source to perform table scan by

        Returns: A list of matching replication confgs.
        """
        start_time = time.time()
        filter_exp = Attr(REPL_SOURCE_ATTR_NAME).eq(source)
        response = self._config_repl_table.scan(FilterExpression=filter_exp)
        configs: List[ReplicationConfig] = self.__map_results(response)

        while 'LastEvaluatedKey' in response:
            response = self._config_repl_table.scan(FilterExpression=filter_exp,
                                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            configs = configs + self.__map_results(response)

        log.info(f"Returning {len(configs)} parameter names from dynamo cache after "
                 f"{time.time() - start_time} seconds.")

        return configs

    def get_config_repl(self, destination: str) -> ReplicationConfig:
        """
        Lookup a replication config by destination
        Args:
            destination: str: /app/demo-time/replicated/destination/path

        Returns: Matching replication config, or None if none match.
        """

        filter_exp = Key(REPL_DEST_KEY_NAME).eq(destination)
        result = self._config_repl_table.query(KeyConditionExpression=filter_exp)

        if "Items" in result and len(result["Items"]) > 0:
            items = result["Items"][0]

            config = ReplicationConfig(
                items[REPL_DEST_KEY_NAME],
                RunEnv(items.get(REPL_RUN_ENV_KEY_NAME, "unknown")),
                items[REPL_NAMESPACE_ATTR_NAME],
                items[REPL_SOURCE_ATTR_NAME],
                ReplicationType(items[REPL_TYPE_ATTR_NAME]),
                user=items[REPL_USER_ATTR_NAME],
            )
            return config
        else:
            return None

    def put_config_repl(self, config: ReplicationConfig) -> None:
        """
        Stores a replication configuration
        Args:
            config: ReplicationConfig -> a hydrated replication config object.
        """
        item = {
            REPL_DEST_KEY_NAME: config.destination,
        }

        for key in config.props:
            if (
                    key != REPL_DEST_KEY_NAME
                    and not isinstance(config.props[key], float)
            ):
                item[key] = config.props[key]
            elif isinstance(config.props[key], float):
                item[key] = Decimal(f"{config.props[key]}")

        self._config_repl_table.put_item(Item=item)

    def get_audit_logs(self, ps_name: str) -> List[AuditLog]:
        """
        Args:
            ps_name: /path/to/parameter to query audit logs for.

        Returns: List[AuditLog]. Logs that match for the /ps/name in ParameterStore.
        """
        filter_exp = Key(AUDIT_PARAMETER_KEY_NAME).eq(ps_name)
        result = self._audit_table.query(KeyConditionExpression=filter_exp)
        items = result.get('Items', None)

        audit_logs: List[AuditLog] = []
        if items is not None:
            for item in items:
                log = AuditLog(item[AUDIT_PARAMETER_KEY_NAME], item[AUDIT_TIME_KEY_NAME],
                               item[AUDIT_ACTION_ATTR_NAME], item[AUDIT_USER_ATTR_NAME])
                audit_logs.append(log)

        return audit_logs

    def get_all_config_names(self, prefix: str = None, one_level: bool = False, start_key: str = None) -> Set[str]:
        """
        Retrieve all key names from the Dynamo DB config-cache table in each account. Much more efficient than
        querying SSM directly.
        Args:
            start_key: Optional: Is used for recursive paginated lookups to get the full data set. This should not
            be passed in by the user.
        Returns:

        """
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

        if one_level:
            configs = set(filter(lambda x: len(x.split('/')) == len(prefix.split('/')) + 1, configs))

        if 'LastEvaluatedKey' in result:
            configs = configs | self.get_all_config_names(prefix, one_level, start_key=result['LastEvaluatedKey'])

        log.info(
            f"Returning config names from dynamo cache after: {time.time() - start_time} "
            f"seconds with {len(configs)} configs.")
        return configs

    def get_config_names_after(self, millis_since_epoch: int) -> Set[ConfigItem]:
        """
        Retrieve all key names from the Dynamo DB config-cache table in each account. Much more efficient than
        querying SSM directly.
        Args:
            millis_since_epoch: milliseconds in epoch to lookup config names from cache after
        Returns: Set[str] -> configs that have been added to cache table after millis_since_epoch
        """

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
            configs.add(ConfigItem.from_dict(item))

        log.info(f"Returning {len(configs)} parameter names from dynamo cache after time: [{millis_since_epoch}] in "
                 f"{time.time() - start_time} seconds.")

        return configs

    def put_in_config_cache(self, name):
        item = {
            CACHE_PARAMETER_KEY_NAME: name
        }

        self._cache_table.put_item(Item=item)

    ## Mapping and utilities Todo: Should be moved / refactored
    def __cfg_from_boto_item(self, boto_item: Dict) -> ReplicationConfig:
        return ReplicationConfig(
            boto_item[REPL_DEST_KEY_NAME],
            RunEnv(boto_item.get(REPL_RUN_ENV_KEY_NAME, "unknown")),
            boto_item[REPL_NAMESPACE_ATTR_NAME],
            boto_item[REPL_SOURCE_ATTR_NAME],
            ReplicationType(boto_item[REPL_TYPE_ATTR_NAME]),
        )

    def __map_results(self, result: dict) -> List[ReplicationConfig]:
        """
        Takes a DDB Result object with a single result and maps it into a replication config
        Args:
            result: DDB boto3 result obj

        Returns: ReplicationConfig if result is found, else and empty list

        """
        repl_cfgs = []
        if "Items" in result and len(result["Items"]) > 0:
            for item in result["Items"]:
                repl_cfgs.append(self.__cfg_from_boto_item(item))

        return repl_cfgs