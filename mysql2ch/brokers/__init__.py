import abc

from mysql2ch.settings import Settings


class Broker:
    @classmethod
    @abc.abstractmethod
    def init(cls, settings: Settings):
        cls.settings = settings

    @abc.abstractmethod
    def send(self, schema: str, msg: dict):
        """
        send msg to broker
        """
        raise NotImplementedError

    @abc.abstractmethod
    def msgs(self, schema: str, last_msg_id, block: int = None):
        """
        get msgs from broker
        """
        raise NotImplementedError

    @abc.abstractmethod
    def commit(
        self, schema: str,
    ):
        """
        commit mgs
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self,):
        raise NotImplementedError
