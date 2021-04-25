import time

from enum import Enum
from pydantic import Field, validator, BaseModel


class ConfigState(Enum):
    DELETED = 0
    ACTIVE = 1


class ConfigItem(BaseModel):
    name: str = Field(None, alias='parameter_name')
    state: ConfigState
    last_updated: int = time.time()

    @validator('state', pre=True)
    def init_state(cls, value):
        return ConfigState[value]

    def __lt__(self, o: "ConfigItem") -> bool:
        return self.last_updated < o.last_updated

    def __hash__(self):
        return hash(f'{self.name}-{self.state.name}-{self.last_updated}')
