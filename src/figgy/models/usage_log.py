import time
from typing import Optional

from pydantic import BaseModel


class UsageLog(BaseModel):
    """
    Represents a singular usage log retrieved from our config-usage-tracker table.
    """
    parameter_name: str
    last_updated: int
    user: str

    def __gt__(self, other):
        if type(other) == UsageLog:
            return self.last_updated > other.last_updated
        else:
            return self.last_updated > other

    def __lt__(self, other):
        if type(other) == UsageLog:
            return self.last_updated < other.last_updated
        else:
            return self.last_updated > other

    def __hash__(self):
        return hash(f'{self.parameter_name}-{self.last_updated}-{self.user}')