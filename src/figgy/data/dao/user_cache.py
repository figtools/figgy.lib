import time
from typing import Set

from figgy.constants.data import *


class UserCacheDao:
    """
    Supports operations on the figgy-user-cache ddb table.
    """

    def __init__(self, dynamo_resource):
        self._dynamo_resource = dynamo_resource
        self._table = self._dynamo_resource.Table(USER_CACHE_TABLE_NAME)

    def add_user_to_cache(self, name: str, state=USER_CACHE_STATE_ACTIVE, timestamp: int = 0):
        """
         Stores a user in the cache table.
        :param name: Name of parameter to store
        :param state: state to set in the cache
        :param timestamp: Time of the event that triggers the insert, or now() if not supplied.
        """

        timestamp = timestamp if timestamp else int(time.time() * 1000)

        item = {
            USER_CACHE_PARAM_NAME_KEY: name,
            USER_CACHE_STATE_ATTR_NAME: state,
            USER_CACHE_LAST_UPDATED_KEY: timestamp
        }

        self._table.put_item(Item=item)

    def get_all_users(self) -> Set[str]:
        """
        Select all user names from the user cache table.
        """

        response = self._table.scan()

        items = response.get('Items', [])
        all_users = set([item.get('user_name') for item in items])

        while 'LastEvaluatedKey' in response:
            response = self._table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            all_users = all_users | set([item.get('user_name') for item in items])

        return all_users
