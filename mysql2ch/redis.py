import json

import redis
from redis.sentinel import Sentinel

from mysql2ch.common import JsonEncoder, object_hook
from mysql2ch.settings import Settings


class Redis:
    @classmethod
    def init(cls, settings: Settings):
        """
        init setting and create redis instance
        """
        cls.settings = settings
        if settings.redis_sentinel:
            sentinel = Sentinel(sentinels=settings.redis_sentinel_hosts,)
            kwargs = dict(
                service_name=settings.redis_sentinel_master,
                password=settings.redis_password,
                decode_responses=True,
            )
            cls.master = sentinel.master_for(**kwargs)
            cls.slave = sentinel.slave_for(**kwargs)
        else:
            pool = redis.ConnectionPool(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
            )
            cls.master = cls.slave = redis.StrictRedis(connection_pool=pool)


class RedisBroker(Redis):
    last_msg_id: str = "0"

    def _get_queue(self, schema: str):
        return f"{self.settings.redis_prefix}:schema:{schema}"

    def send(self, schema: str, msg: dict):
        self.master.xadd(
            self._get_queue(schema),
            {"msg": json.dumps(msg, cls=JsonEncoder)},
            maxlen=self.settings.queue_max_len,
        )

    def msgs(self, schema: str, last_msg_id: str = None):
        if not last_msg_id:
            self.last_msg_id = self._get_last_msg_id(schema)
        while True:
            for _, msgs in self.slave.xread({self._get_queue(schema): self.last_msg_id}):
                for msg_id, msg in msgs:
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
        return f"{self.settings.redis_prefix}:last_msg_id"

    def commit(
        self, schema: str,
    ):
        """
        commit msgs
        :param schema:
        :return:
        """
        self.master.hset(self._get_last_msg_id_key(), schema, self.last_msg_id)


class RedisLogPos(Redis):
    def __init__(self):
        self.server_id = self.settings.mysql_server_id
        self.key = f"{self.settings.redis_prefix}:binlog:{self.server_id}"

    def set_log_pos_master(
        self, master_host, master_port, relay_master_log_file, exec_master_log_pos
    ):
        self.master.hmset(
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
        self.master.hmset(self.key, {"log_pos": log_pos, "log_file": log_file})

    def get_log_pos(self):
        """
        get binlog pos
        :return:
        """
        log_position = self.slave.hgetall(self.key)
        return log_position.get("log_file"), log_position.get("log_pos")
