import redis
from redis.sentinel import Sentinel

from synch.settings import Settings


class Redis:
    settings: Settings
    master: redis.Redis
    slave: redis.Redis

    def __init__(self, settings: Settings):
        """
        init setting and create redis instance
        """
        self.settings = settings
        if settings.redis_sentinel:
            sentinel = Sentinel(sentinels=settings.redis_sentinel_hosts,)
            kwargs = dict(
                service_name=settings.redis_sentinel_master,
                password=settings.redis_password,
                decode_responses=True,
            )
            self.master = sentinel.master_for(**kwargs)
            self.slave = sentinel.slave_for(**kwargs)
        else:
            pool = redis.ConnectionPool(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
            )
            self.master = self.slave = redis.StrictRedis(connection_pool=pool)

    def close(self):
        self.master.close()
        self.slave.close()


class RedisLogPos(Redis):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.server_id = self.settings.mysql_server_id
        self.pos_key = f"{self.settings.redis_prefix}:binlog:{self.server_id}"

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
