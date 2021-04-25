import time
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, validator

from figgy.constants.data import SSM_DELETE, SSM_PUT, SSM_SECURE_STRING, SSM_STRING


class AuditLog(BaseModel):
    # Todo later move to enums and updated all references
    class Action:
        DELETE = SSM_DELETE
        PUT = SSM_PUT

    class Type:
        STRING = SSM_STRING
        SECURE_STRING = SSM_SECURE_STRING

    """
    Represents a singular audit log retrieved from our config-auditor table.
    """
    parameter_name: str
    time: int
    action: str
    user: str
    value: Optional[str]
    type: Optional[str]
    description: Optional[str]
    version: Optional[int]
    key_id: Optional[str]

    def pretty_print(self):
        return f"Parameter: {self.parameter_name}\r\n" \
               f"Time: {time.ctime(int(self.time / 1000))}\r\n" \
               f"User: {self.user}\r\n" \
               f"Action: {self.action}\r\n"

    def __gt__(self, other):
        return self.time > other.time

    def __lt__(self, other):
        return self.time < other.time

    def __hash__(self):
        return hash(f'{self.parameter_name}-{self.time}-{self.action}-{self.user}')
