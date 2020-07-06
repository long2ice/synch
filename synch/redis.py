import redis
from redis.sentinel import Sentinel

from synch.settings import Settings


class Redis:
    master: redis.Redis
    slave: redis.Redis

    def __init__(self):
        """
        init setting and create redis instance
        """
        settings = Settings.get("redis")
        self.prefix = settings.get("prefix")
        self.queue_max_len = settings.get("queue_max_len")
        self.sentinel = settings.get("sentinel")
        if self.sentinel:
            sentinel = Sentinel(
                sentinels=map(lambda x: x.split(":"), settings.get("sentinel_hosts"))
            )
            kwargs = dict(
                service_name=settings.get("sentinel_master"),
                password=settings.get("password"),
                decode_responses=True,
            )
            self.master = sentinel.master_for(**kwargs)
            self.slave = sentinel.slave_for(**kwargs)
        else:
            pool = redis.ConnectionPool(
                host=settings.get("host"),
                port=settings.get("port"),
                db=settings.get("db"),
                password=settings.get("password"),
                decode_responses=True,
            )
            self.master = self.slave = redis.StrictRedis(connection_pool=pool)

    def close(self):
        self.master.close()
        self.slave.close()


class RedisLogPos(Redis):
    def __init__(
        self, alias: str,
    ):
        super().__init__()
        self.server_id = Settings.get_source_db(alias).get("server_id")
        self.pos_key = f"{self.prefix}:binlog:{alias}:{self.server_id}"

    def set_log_pos_master(
        self, master_host, master_port, relay_master_log_file, exec_master_log_pos
    ):
        self.master.hmset(
            self.pos_key,
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
        self.master.hmset(self.pos_key, {"log_pos": log_pos, "log_file": log_file})

    def get_log_pos(self):
        """
        get binlog pos
        :return:
        """
        log_position = self.slave.hgetall(self.pos_key)
        return log_position.get("log_file"), log_position.get("log_pos")
