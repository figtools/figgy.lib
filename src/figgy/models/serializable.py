from enum import Enum
from typing import Dict

from pydantic.main import BaseModel


class Serializable(BaseModel):

    def dict(self, **kwargs):
        # Convert enums to valid strings when exporting as dict.
        output_dict: Dict = super().dict(**kwargs)
        for key, value in output_dict.items():
            if isinstance(value, Enum):
                output_dict[key] = value.value

        return output_dict
