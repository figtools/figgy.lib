from typing import Union

from figgy.data.dao.ssm import SsmDao
from figgy.models.fig import Fig
from figgy.utils.utils import Utils


class FigService:

    def __init__(self, ssm_dao: SsmDao):
        self._ssm = ssm_dao

    def get(self, name: str, version: int = 0) -> Fig:
        """ Version is defaulted to 0 which will return latest """
        details, is_latest_version = self._ssm.get_parameter_details(name, version)
        return Fig(**details, is_latest_version=is_latest_version)

    def get_simple(self, name: str) -> Fig:
        value = self._ssm.get_parameter(name)
        return Fig(name=name, value=value, is_latest_version=True)

    def save(self, fig: Fig):
        self._ssm.set_parameter(
            key=fig.name,
            value=fig.value,
            desc=fig.description,
            type=fig.type.value if fig.type else None,
            key_id=fig.kms_key_id
        )

    def set(self, fig: Fig):
        self.save(fig)

    def delete(self, fig: Union[Fig, str]):
        Utils.validate_set(fig, 'Fig Name')
        name = fig.name if isinstance(fig, Fig) else fig
        self._ssm.delete_parameter(name)
