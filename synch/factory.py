import logging
import sys
from typing import Optional

from synch.broker import Broker
from synch.broker.kafka import KafkaBroker
from synch.broker.redis import RedisBroker
from synch.reader import Reader
from synch.reader.mysql import Mysql
from synch.reader.postgres import Postgres
from synch.replication.clickhouse import ClickHouseWriter
from synch.settings import BrokerType, Settings, SourceDatabase


class Global:
    """
    global instances
    """

    settings: Optional[Settings] = None
    reader: Optional[Reader] = None
    writer: Optional[ClickHouseWriter] = None
    broker: Optional[Broker] = None

    @classmethod
    def init(cls, config):
        cls.settings = Settings.parse(config)
        broker_type = cls.settings.broker_type
        if broker_type == BrokerType.redis.value:
            cls.broker = RedisBroker(cls.settings)
        elif broker_type == BrokerType.kafka.value:
            cls.broker = KafkaBroker(cls.settings)

        cls.writer = ClickHouseWriter(
            host=cls.settings.clickhouse_host,
            port=cls.settings.clickhouse_port,
            password=cls.settings.clickhouse_password,
            user=cls.settings.clickhouse_user,
        )
        if cls.settings.source_db == SourceDatabase.mysql.value:
            cls.reader = Mysql(cls.settings)
        elif cls.settings.source_db == SourceDatabase.postgres.value:
            cls.reader = Postgres(cls.settings)


def init_logging(debug):
    """
    init logging config
    :param debug:
    :return:
    """
    base_logger = logging.getLogger("synch")
    if debug:
        base_logger.setLevel(logging.DEBUG)
    else:
        base_logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    base_logger.addHandler(sh)


def init(config_file):
    Global.init(config_file)
    settings = Global.settings
    init_logging(settings.debug)
    if settings.sentry_dsn:
        import sentry_sdk
        from sentry_sdk.integrations.redis import RedisIntegration

        sentry_sdk.init(
            settings.sentry_dsn, environment=settings.environment, integrations=[RedisIntegration()]
        )
