import abc


class Broker:
    def __init__(self, alias: str):
        self.alias = alias

    @abc.abstractmethod
    def send(self, schema: str, msg: dict):
        """
        send msg to broker
        """
        raise NotImplementedError

    @abc.abstractmethod
    def msgs(self, schema: str, last_msg_id, count: int = None, block: int = None):
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
