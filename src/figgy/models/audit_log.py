import time
from typing import Optional

from pydantic import BaseModel


class AuditLog(BaseModel):
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

    def __str__(self):
        return f"Parameter: {self.parameter_name}\r\n" \
               f"Time: {time.ctime(int(self.time / 1000))}\r\n" \
               f"User: {self.user}\r\n" \
               f"Action: {self.action}\r\n"

    def __gt__(self, other):
        return self.time > other.time