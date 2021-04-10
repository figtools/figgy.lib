from pydantic.decorator import BaseModel


class StringSerializable(BaseModel):
    """
        Classes extending (implementing) StringSerializable should implement serialization to a single string
        by overriding the dict() and json() methods.
    """

    def dict(self, **kwargs):
        pass
