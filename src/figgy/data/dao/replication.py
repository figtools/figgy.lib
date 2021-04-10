import time
import logging

from typing import List, Optional

from boto3.dynamodb.conditions import Attr, Key

from figgy.constants.data import *
from figgy.models.replication_config import ReplicationConfig

log = logging.getLogger(__name__)


class ReplicationDao:

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._config_repl_table = self._dynamo_resource.Table(REPL_TABLE_NAME)

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
        configs = [ReplicationConfig(**item) for item in result.get('Items', [])]

        while 'LastEvaluatedKey' in result:
            result = self._config_repl_table.scan(FilterExpression=filter_exp, ExclusiveStartKey=start_key)
            configs = configs + [ReplicationConfig(**item) for item in result.get('Items', [])]

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

    def get_config_repl(self, destination: str) -> Optional[ReplicationConfig]:
        """
        Lookup a replication config by destination
        Args:
            destination: str: /app/demo-time/replicated/destination/path

        Returns: Matching replication config, or None if none match.
        """

        filter_exp = Key(REPL_DEST_KEY_NAME).eq(destination)
        result = self._config_repl_table.query(KeyConditionExpression=filter_exp)

        if "Items" in result and len(result["Items"]) > 0:
            item = result["Items"][0]
            log.info(f'Got item: {item}')
            return ReplicationConfig(**item)
        else:
            return None

    def put_config_repl(self, config: ReplicationConfig) -> None:
        """
        Stores a replication configuration
        Args:
            config: ReplicationConfig -> a hydrated replication config object.
        """
        item = config.dict()
        item['run_env'] = item['run_env']['env']  # Convert env to str for ddb model format.

        self._config_repl_table.put_item(Item=item)

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
                repl_cfgs.append(ReplicationConfig(**item))

        return repl_cfgs

    def delete_config(self, destination: str) -> None:
        """
        Deletes a Replication configuration from the DB
        Args:
            destination: str -> /path/to/configuration/destination
        """
        self._config_repl_table.delete_item(
            Key={REPL_DEST_KEY_NAME: destination}
        )