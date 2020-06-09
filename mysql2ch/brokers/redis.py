from typing import Optional

import redis

from mysql2ch.brokers import Broker
from mysql2ch.settings import Settings


class RedisBroker(Broker):

    def __init__(self, settings: Settings):
        super().__init__(settings)
        pool = redis.ConnectionPool(
            host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, password=settings.redis_password,
            decode_responses=True
        )
        self.redis = redis.StrictRedis(connection_pool=pool)

    def send(self, schema: str, msg: dict):
        queue = f'{self.settings.redis_prefix}:{schema}'
        self.redis.xadd(queue, msg)

    def msgs(self, schema: str, msg_from: str, offset: Optional[int] = None):
        queue = f'{self.settings.redis_prefix}:{schema}'
        while True:
            for msg in self.redis.xread(queue, self.settings.insert_num):
                yield msg

    def commit(self):
        pass
