import datetime
import logging

from typing import Optional, List, Dict

from boto3.dynamodb.conditions import Key, Attr

from figgy.constants.data import *
from figgy.models.audit_log import AuditLog
from figgy.models.parameter_history import ParameterHistory
from figgy.models.parameter_store_history import PSHistory
from figgy.models.restore_config import RestoreConfig

log = logging.getLogger(__name__)


class AuditDao:

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._audit_table = self._dynamo_resource.Table(AUDIT_TABLE_NAME)

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

    def get_audit_logs(self, ps_name: str, before: Optional[int] = None, after: Optional[int] = None) -> List[AuditLog]:
        """
        Args:
            ps_name: /path/to/parameter to query audit logs for.

        Returns: List[AuditLog]. Logs that match for the /ps/name in ParameterStore.
        """
        key_expr = Key(AUDIT_PARAMETER_KEY_NAME).eq(ps_name)

        if before and not after:
            key_expr = key_expr & Key(AUDIT_TIME_KEY_NAME).lt(before)
        elif before and after:
            key_expr = key_expr & Key(AUDIT_TIME_KEY_NAME).between(before, after)
        elif after and not before:
            key_expr = key_expr & Key(AUDIT_TIME_KEY_NAME).gt(after)

        result = self._audit_table.query(KeyConditionExpression=key_expr)
        items = result.get('Items', None)

        audit_logs: List[AuditLog] = []
        if items is not None:
            for item in items:
                log = AuditLog(**item)
                audit_logs.append(log)

        return audit_logs

    def find_logs(self, filter: str = None, parameter_type: str = None,
                  before: int = None, after: int = None, action: str = SSM_PUT) -> List[AuditLog]:

        log.info(f'Inputs: Filter: {filter}, param_type: {parameter_type}, before: {before} after: {after}')

        filter_exp = Attr(AUDIT_ACTION_ATTR_NAME).eq(action)

        if parameter_type:
            log.info(f"Adding type: {parameter_type}")
            filter_exp = filter_exp & Attr(AUDIT_PARAMETER_ATTR_TYPE).eq(parameter_type)

        if filter:
            log.info(f"Adding filter: {filter}")
            filter_exp = filter_exp & (Attr(AUDIT_PARAMETER_KEY_NAME).contains(filter) | Attr(AUDIT_PARAMETER_ATTR_USER).contains(filter))

        if before:
            log.info(f"Adding before: {before}")
            filter_exp = filter_exp & (Attr(AUDIT_TIME_KEY_NAME).lt(int(before)))

        if after:
            log.info(f"Adding after: {after}")
            filter_exp = filter_exp & Attr(AUDIT_TIME_KEY_NAME).gt(int(after))

        response = self._audit_table.scan(
            FilterExpression=filter_exp
        )

        items = response.get('Items', [])
        log.info(f'First items: {items}')

        while 'LastEvaluatedKey' in response:
            response = self._audit_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            log.info(f"Getting more items, got response: {response}")
            items = items + response.get('Items', [])

        logs: Dict[str, List[AuditLog]] = {}

        # Add to dict so we can sort to find latest of each type of config
        for item in items:
            name = item.get(AUDIT_PARAMETER_KEY_NAME)
            new_item = AuditLog(**item)
            logs[name] = logs.get(name) + [new_item] if logs.get(name) else [new_item]

        # Sort, latest log should be at position 0
        for key, value in logs.items():
            logs[key] = sorted(value, reverse=True)

        # return latest items
        latest = [logs[key][0] for key, value in logs.items()]

        log.info(f"Found latest stale configs: {latest}")

        return latest
