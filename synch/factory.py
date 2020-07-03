import logging
import sys
from typing import Optional

from synch.broker import Broker
from synch.enums import BrokerType
from synch.settings import Settings


class Global:
    """
    global instances
    """

    settings: Optional[Settings] = None
    broker: Optional[Broker] = None

    @classmethod
    def init(cls, config_file):
        cls.settings = Settings(config_file)
        broker_type = cls.settings.broker_type
        if broker_type == BrokerType.redis.value:
            from synch.broker.redis import RedisBroker

            cls.broker = RedisBroker(cls.settings.get("redis"))
        elif broker_type == BrokerType.kafka.value:
            from synch.broker.kafka import KafkaBroker

            cls.broker = KafkaBroker(cls.settings)


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
    """
    init
    """
    Global.init(config_file)
    settings = Global.settings
    init_logging(settings.debug)
    dsn = settings.get("sentry", "dsn")
    if dsn:
        import sentry_sdk
        from sentry_sdk.integrations.redis import RedisIntegration

        sentry_sdk.init(
            dsn,
            environment=settings.get("sentry", "environment"),
            integrations=[RedisIntegration()],
        )
