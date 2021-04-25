import logging
import time
from typing import List, Dict, Generator, Iterable

from boto3.dynamodb.conditions import Attr, Key

from figgy.constants.data import *
from figgy.models.usage_log import UsageLog

log = logging.getLogger(__name__)


class UsageTrackerDao:

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._table = self._dynamo_resource.Table(CONFIG_USAGE_TABLE_NAME)

    def add_usage_log(self, parameter_name: str, user: str, timestamp: int = int(time.time() * 1000)):
        item = {
            CONFIG_USAGE_PARAMETER_KEY: parameter_name,
            CONFIG_USAGE_USER_KEY: user,
            CONFIG_USAGE_LAST_UPDATED_KEY: timestamp,
            CONFIG_USAGE_EMPTY_IDX_KEY: CONFIG_USAGE_EMPTY_IDX_VALUE
        }

        self._table.put_item(Item=item)

    def find_logs_by_user(self, user: str, filter: str = None) -> Iterable[UsageLog]:
        log.info(f'Finding usage logs for user: {user}')
        query_expr = Key(CONFIG_USAGE_USER_KEY).eq(user) & Key(CONFIG_USAGE_LAST_UPDATED_KEY).gt(0)
        filter_expr = None

        if filter:
            filter_expr = Attr(CONFIG_USAGE_PARAMETER_KEY).contains(filter)

        if filter_expr:
            response = self._table.query(
                IndexName=CONFIG_USAGE_USER_LAST_UPDATED_IDX,
                KeyConditionExpression=query_expr,
                FilterExpression=filter_expr
            )
        else:
            response = self._table.query(
                IndexName=CONFIG_USAGE_USER_LAST_UPDATED_IDX,
                KeyConditionExpression=query_expr,
            )

        items = response.get('Items', [])
        usage_logs = [UsageLog(**item) for item in items]
        for usage_log in usage_logs:
            yield usage_log

        while 'LastEvaluatedKey' in response:
            if filter_expr:
                response = self._table.query(IndexName=CONFIG_USAGE_USER_LAST_UPDATED_IDX,
                                             ExclusiveStartKey=response['LastEvaluatedKey'],
                                             KeyConditionExpression=query_expr,
                                             FilterExpression=filter_expr)
            else:
                response = self._table.query(IndexName=CONFIG_USAGE_USER_LAST_UPDATED_IDX,
                                             ExclusiveStartKey=response['LastEvaluatedKey'],
                                             KeyConditionExpression=query_expr)


            items = items + response.get('Items', [])
            for usage_log in [UsageLog(**item) for item in items]:
                yield usage_log

    def find_logs_by_time(self, before: int = None, after: int = None, filter: str = None,
                          latest_log_only=True, exclude_names: List[str] = None) -> Iterable[UsageLog]:
        """
        Yields UsageLogs incrementally as a Scan operation is completed. Yields occur incrementally
        to reduce memory overhead.
        """

        log.info(f'Inputs: before: {before} after: {after}')
        query_expr = Key(CONFIG_USAGE_EMPTY_IDX_KEY).eq(CONFIG_USAGE_EMPTY_IDX_VALUE)
        filter_exp = None

        if before:
            log.info(f"Adding before: {before}")
            query_expr = query_expr & Key(CONFIG_USAGE_LAST_UPDATED_KEY).lt(before)

        if after:
            log.info(f"Adding after: {after}")
            query_expr = query_expr & Key(CONFIG_USAGE_LAST_UPDATED_KEY).gt(after)

        if filter:
            filter_exp = Attr(CONFIG_USAGE_PARAMETER_KEY).contains(filter) | \
                         Attr(CONFIG_USAGE_USER_KEY).contains(filter)

        if filter_exp:
            response = self._table.query(
                IndexName=CONFIG_USAGE_LAST_UPDATED_ONLY_IDX,
                KeyConditionExpression=query_expr,
                FilterExpression=filter_exp
            )
        else:
            response = self._table.query(
                IndexName=CONFIG_USAGE_LAST_UPDATED_ONLY_IDX,
                KeyConditionExpression=query_expr,
            )

        items = response.get('Items', [])
        matching_logs = [UsageLog(**item) for item in items]
        matching_logs = self.__filter_names_from_logs(matching_logs, exclude_names)
        log.info(f'Yielding {matching_logs}')
        for matching_log in matching_logs:
            yield matching_log

        while 'LastEvaluatedKey' in response:
            if filter_exp:
                response = self._table.query(IndexName=CONFIG_USAGE_LAST_UPDATED_ONLY_IDX,
                                             ExclusiveStartKey=response['LastEvaluatedKey'],
                                             KeyConditionExpression=query_expr,
                                             FilterExpression=filter_exp)
            else:
                response = self._table.query(IndexName=CONFIG_USAGE_LAST_UPDATED_ONLY_IDX,
                                             ExclusiveStartKey=response['LastEvaluatedKey'],
                                             KeyConditionExpression=query_expr)

            items = items + response.get('Items', [])
            matching_logs = [UsageLog(**item) for item in items]
            matching_logs = self.__filter_names_from_logs(matching_logs, exclude_names)
            for matching_log in matching_logs:
                yield matching_log

    def __filter_names_from_logs(self, logs: List[UsageLog], exclude_names: List[str]):
        if exclude_names:
            return [log for log in logs if log.parameter_name not in exclude_names]
        else:
            return logs
