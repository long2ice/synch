import logging
import sys
from typing import Dict

from synch.broker import Broker
from synch.broker.kafka import KafkaBroker
from synch.broker.redis import RedisBroker
from synch.enums import BrokerType, ClickHouseEngine, SourceDatabase
from synch.reader import Reader
from synch.settings import Settings
from synch.writer import ClickHouse
from synch.writer.collapsing_merge_tree import ClickHouseCollapsingMergeTree
from synch.writer.merge_tree import ClickHouseMergeTree

_readers: Dict[str, Reader] = {}
_writers: Dict[str, ClickHouse] = {}
_brokers: Dict[str, Broker] = {}


def get_reader(alias: str):
    """
    get reader once
    """
    r = _readers.get(alias)
    if not r:
        source_db = Settings.get_source_db(alias)
        if not source_db:
            raise Exception(f"Can't find alias {alias} in config.")
        db_type = source_db.get("db_type")
        if db_type == SourceDatabase.mysql.value:
            from synch.reader.mysql import Mysql

            r = Mysql(alias)
        elif db_type == SourceDatabase.postgres.value:
            from synch.reader.postgres import Postgres

            r = Postgres(alias)
        else:
            raise NotImplementedError(f"Unsupported db_type {db_type}")
        _readers[alias] = r
    return r


def get_writer(engine: ClickHouseEngine = None):
    """
    get writer once
    """
    w = _writers.get(engine)
    if not w:
        settings = Settings.get("clickhouse")
        if engine == ClickHouseEngine.merge_tree.value or engine is None:
            w = ClickHouseMergeTree(settings)
        elif engine == ClickHouseEngine.collapsing_merge_tree:
            w = ClickHouseCollapsingMergeTree(settings)
        _writers[engine] = w
    return w


def get_broker(alias: str):
    b = _brokers.get(alias)
    broker_type = Settings.get_source_db(alias).get("broker_type")
    if not b:
        if broker_type == BrokerType.redis:
            b = RedisBroker(alias)
        elif broker_type == BrokerType.kafka:
            b = KafkaBroker(alias)
        else:
            raise NotImplementedError(f"Unsupported broker_type {broker_type}")
        _brokers[alias] = b
    return b


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
    Settings.init(config_file)
    init_logging(Settings.debug())
    dsn = Settings.get("sentry", "dsn")
    if dsn:
        import sentry_sdk
        from sentry_sdk.integrations.redis import RedisIntegration

        sentry_sdk.init(
            dsn,
            environment=Settings.get("sentry", "environment"),
            integrations=[RedisIntegration()],
        )
