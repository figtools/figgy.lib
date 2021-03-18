import time
from enum import Enum
from typing import Dict

from pydantic import dataclasses

from figgy.constants.data import *


class ConfigState(Enum):
    DELETED = 0
    ACTIVE = 1


@dataclasses.dataclass(frozen=True)
class ConfigItem:
    name: str
    state: ConfigState
    last_updated: int

    @staticmethod
    def from_dict(obj: Dict) -> "ConfigItem":
        name = obj.get(CACHE_PARAMETER_KEY_NAME, None)
        last_updated = obj.get(CACHE_LAST_UPDATED_KEY_NAME, time.time())
        state = obj.get(CACHE_STATE_ATTR_NAME, None)

        return ConfigItem(name=name, last_updated=last_updated, state=ConfigState[state])

    def __lt__(self, o: "ConfigItem") -> bool:
        return self.last_updated < o.last_updated
