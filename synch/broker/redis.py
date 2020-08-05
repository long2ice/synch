import json

from synch.broker import Broker
from synch.common import JsonEncoder, object_hook
from synch.redis import Redis


class RedisBroker(Broker, Redis):
    last_msg_id: str = "0"

    def __init__(self, alias: str):
        Broker.__init__(self, alias)
        Redis.__init__(self)

    def _get_queue(self, schema: str):
        return f"{self.prefix}:{self.alias}:{schema}"

    def send(self, schema: str, msg: dict):
        self.master.xadd(
            self._get_queue(schema),
            {"msg": json.dumps(msg, cls=JsonEncoder)},
            maxlen=self.queue_max_len,
        )

    def msgs(self, schema: str, last_msg_id=None, count: int = None, block: int = None):
        if not last_msg_id:
            self.last_msg_id = self._get_last_msg_id(schema)
        while True:
            msgs_item = self.slave.xread(
                {self._get_queue(schema): self.last_msg_id}, count=count, block=block
            )
            if not msgs_item:
                yield None, msgs_item
            else:
                for msg_item in msgs_item[0][1]:
                    msg_id, msg = msg_item
                    self.last_msg_id = msg_id
                    yield msg_id, json.loads(msg.get("msg"), object_hook=object_hook)

    def _get_last_msg_id(self, schema: str):
        """
        get last msg id
        :return:
        """
        return (
            self.last_msg_id
            if self.last_msg_id != "0"
            else self.slave.hget(self._get_last_msg_id_key(), schema) or "0"
        )

    def _get_last_msg_id_key(self):
        return f"{self.prefix}:{self.alias}:last_msg_id"

    def commit(
        self, schema: str,
    ):
        """
        commit msgs
        :param schema:
        :return:
        """
        self.master.hset(self._get_last_msg_id_key(), schema, self.last_msg_id)

    def close(self):
        self.master and self.master.close()
        self.slave and self.slave.close()
