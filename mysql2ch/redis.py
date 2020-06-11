import json
from typing import List

import redis

from mysql2ch.common import JsonEncoder, object_hook
from mysql2ch.settings import Settings


class Redis:
    def __init__(self, settings: Settings):
        self.settings = settings
        pool = redis.ConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=True,
        )
        self.redis = redis.StrictRedis(connection_pool=pool)


class RedisBroker(Redis):
    def _get_queue(self, schema: str):
        return f"{self.settings.redis_prefix}:schema:{schema}"

    def send(self, schema: str, msg: dict):
        self.redis.xadd(
            self._get_queue(schema),
            {"msg": json.dumps(msg, cls=JsonEncoder)},
            maxlen=self.settings.queue_max_len,
        )

    def msgs(self, schema: str, last_msg_id: str = None):
        if not last_msg_id:
            last_msg_id = self._get_last_msg_id()
        while True:
            for _, msgs in self.redis.xread({self._get_queue(schema): last_msg_id}):
                for msg_id, msg in msgs:
                    last_msg_id = msg_id
                    yield msg_id, json.loads(msg.get("msg"), object_hook=object_hook)

    def _get_last_msg_id(self):
        """
        get last msg id
        :return:
        """
        return self.redis.get(self._get_last_msg_id_key()) or "0"

    def _get_last_msg_id_key(self):
        return f"{self.settings.redis_prefix}:last_msg_id"

    def commit(self, schema: str, msg_ids: List[str]):
        """
        commit msgs
        :param schema:
        :param msg_ids:
        :return:
        """
        p = self.redis.pipeline()
        p.xack(self._get_queue(schema), schema, *msg_ids)
        p.set(self._get_last_msg_id_key(), msg_ids[-1])
        p.execute()


class RedisLogPos(Redis):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.server_id = settings.mysql_server_id
        self.key = f"{settings.redis_prefix}:binlog:{self.server_id}"

    def set_log_pos_master(
        self, master_host, master_port, relay_master_log_file, exec_master_log_pos
    ):
        self.redis.hmset(
            self.key,
            {
                "master_host": master_host,
                "master_port": master_port,
                "relay_master_log_file": relay_master_log_file,
                "exec_master_log_pos": exec_master_log_pos,
            },
        )

    def set_log_pos_slave(self, log_file, log_pos):
        """
        set binlog pos
        :param log_file:
        :param log_pos:
        :return:
        """
        self.redis.hmset(self.key, {"log_pos": log_pos, "log_file": log_file})

    def get_log_pos(self):
        """
        get binlog pos
        :return:
        """
        log_position = self.redis.hgetall(self.key)
        return log_position.get("log_file"), log_position.get("log_pos")
