from typing import Optional

from pydantic import BaseModel, Field, validator

from figgy.models.enums.fig_type import FigType
from figgy.utils.exceptions import FiggyValidationError


class Fig(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str = Field(None, alias="Name")
    value: str = Field(None, alias="Value")
    description: Optional[str] = Field(None, alias="Description")
    type: Optional[FigType] = Field(None, alias="Type")
    kms_key_id: Optional[str] = Field(None, alias="KeyId")
    version: Optional[str] = Field(None, alias="Version")
    user: Optional[str] = Field(None, alias="LastModifiedUser")
    is_repl_source: Optional[bool] = None
    is_repl_dest: Optional[bool] = None
    is_latest_version: Optional[bool] = None

    @validator("*", pre=True)
    def set_null(cls, value):
        if value == "null":
            value = None

        return value

    @validator("name", "value")
    def validate_not_null(cls, value, values, **kwargs):
        if not value:
            raise FiggyValidationError("Parameter cannot be empty.")

        return value

    def is_missing(self) -> bool:
        return self.name is None or self.value is None
