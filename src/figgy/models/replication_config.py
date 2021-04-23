from enum import Enum

from pydantic import BaseModel, Field, validator

from figgy.constants.models import REPL_TYPES
from figgy.models.run_env import RunEnv
from figgy.constants.data import *
import getpass
from typing import Dict, List, Optional, Union

from figgy.models.serializable import Serializable
from figgy.utils.utils import *


class ReplicationType(Enum):
    """
    Represents the various types of replication configs that are valid for storing into our dynamodb
    `service-config-replication` table
    """
    APP = REPL_TYPE_APP
    MERGE = REPL_TYPE_MERGE

    def __str__(self):
        return self.value


class ReplicationConfig(Serializable):
    """
    This model is used for storing / retrieving data from the `service-config-replication` table.
    """
    destination: str
    run_env: RunEnv = Field(None, alias="env_alias")
    namespace: str
    source: Union[str, List[str]]
    type: ReplicationType
    user: Optional[str]

    @validator('run_env', pre=True, always=True)
    def init_run_env(cls, value, values):
        # this enables us to load via name run_env and env_alias.
        if values.get('run_env'):
            return RunEnv(env=values.get('run_env'))

        if not value:
            value = "unknown"

        return RunEnv(env=value)

    @validator('type', pre=True)
    def init_type(cls, value):
        return ReplicationType(value)

    @validator('user', pre=True, always=True)
    def init_user(cls, value):
        if not value:
            value = getpass.getuser()

        return value

    @staticmethod
    def from_dict(conf: Dict, type: ReplicationType, run_env: RunEnv,
                  namespace: str = None, user: str = None) -> List:
        """
        Dict must be of format - Key (source) -> Value (destination)
        Args:
            conf: Key (repl source) -> Value (repl dest) dictionary
            type: required - Type of replication config (merge / app)
            run_env: RunEnvironment, optional
            namespace: Dest App Namespace, optional
            user: User who is creating this REPL Conf, also optional
        Returns:
            List[ReplicationConfig] - List of hydrated replication config objects based on the parameters.
        """
        cfgs = []
        for key in conf:
            if namespace is None:
                namespace = Utils.parse_namespace(conf[key])
            if user is None:
                user = getpass.getuser()
            cfgs.append(ReplicationConfig(destination=conf[key], source=key, type=type,
                                          run_env=run_env, namespace=namespace, user=user))
        return cfgs

    def __str__(self):
        return f"{self.__dict__}"

    def __hash__(self):
        return hash(f"{self.destination}{self.source}{self.type}")

    def __eq__(self, other):
        if isinstance(other, ReplicationConfig):
            return self.destination == other.destination and self.source == other.source and self.type == other.type
        return False
