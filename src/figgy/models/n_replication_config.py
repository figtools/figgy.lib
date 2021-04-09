from pydantic import validator
from pydantic.main import BaseModel

from figgy.models.replication_config import ReplicationType
from figgy.models.run_env import RunEnv

"""
    Classes prefixed with N are NEW modeled types and are replacing old ones piecemeal.
    
    This class will replace the existing ReplicationConfig and is a Pydantic models which we are
    now using as the standard.
"""


class NReplicationConfig(BaseModel):
    destination: str
    run_env: RunEnv
    namespace: str
    source: str
    type: ReplicationType
    user: str

    @validator('run_env', pre=True)
    def init_env(cls, value):
        if isinstance(value, RunEnv):
            return value
        elif isinstance(value, dict):
            return RunEnv(**value)
        elif isinstance(value, str):
            return RunEnv(env=value)
        else:
            raise ValueError(
                f'Invalid RunEnv property specified. Received: {value}, but must be of type RunEnv or dict.')
