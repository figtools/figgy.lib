from typing import Optional

from pydantic import BaseModel


class RunEnv(BaseModel):
    env: str
    account_id: Optional[str]

    def __str__(self):
        return self.env

    def __eq__(self, obj):
        if isinstance(obj, RunEnv):
            return obj.env == self.env

        return False

    def __hash__(self):
        return hash(self.env)
