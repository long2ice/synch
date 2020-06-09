import abc
from decimal import Decimal
from typing import Optional

import dateutil.parser

from mysql2ch.settings import Settings


class Broker:
    """
    Base class for msg broker
    """
    CONVERTERS = {
        "date": dateutil.parser.parse,
        "datetime": dateutil.parser.parse,
        "decimal": Decimal,
    }

    def __init__(self, settings: Settings):
        self.settings = settings

    def _object_hook(self, obj):
        _spec_type = obj.get("_spec_type")
        if not _spec_type:
            return obj

        if _spec_type in self.CONVERTERS:
            return self.CONVERTERS[_spec_type](obj["val"])
        else:
            raise TypeError("Unknown {}".format(_spec_type))

    @abc.abstractmethod
    def send(self,schema: str, msg: dict):
        """
        send msg to broker
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def msgs(self,  schema: str, msg_from: str, offset: Optional[int] = None):
        """
        consume msg from broker
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self):
        """
        commit msg after consume success
        :return:
        """
        raise NotImplementedError
