import datetime
import json
import logging
from decimal import Decimal
from typing import Dict

import dateutil.parser

from synch.enums import ClickHouseEngine, SourceDatabase
from synch.factory import Global
from synch.reader import Reader
from synch.reader.mysql import Mysql
from synch.reader.postgres import Postgres
from synch.writer import ClickHouse
from synch.writer.collapsing_merge_tree import ClickHouseCollapsingMergeTree
from synch.writer.merge_tree import ClickHouseMergeTree

logger = logging.getLogger("synch.common")

CONVERTERS = {
    "date": dateutil.parser.parse,
    "datetime": dateutil.parser.parse,
    "decimal": Decimal,
}

_readers: Dict[str, Reader] = {}
_writers: Dict[str, ClickHouse] = {}


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {"val": obj.strftime("%Y-%m-%d %H:%M:%S"), "_spec_type": "datetime"}
        elif isinstance(obj, datetime.date):
            return {"val": obj.strftime("%Y-%m-%d"), "_spec_type": "date"}
        elif isinstance(obj, Decimal):
            return {"val": str(obj), "_spec_type": "decimal"}
        else:
            return super().default(obj)


def object_hook(obj):
    _spec_type = obj.get("_spec_type")
    if not _spec_type:
        return obj

    if _spec_type in CONVERTERS:
        return CONVERTERS[_spec_type](obj["val"])
    else:
        raise TypeError("Unknown {}".format(_spec_type))


def get_reader(alias: str):
    """
    get reader once
    """
    reader = _readers.get(alias)
    if not reader:
        source_db = Global.settings.get_source_db(alias)
        if not source_db:
            raise Exception(f"Can't find alias {alias} in config.")
        db_type = source_db.get("db_type")
        if db_type == SourceDatabase.mysql.value:
            reader = Mysql(source_db, Global.settings.get('redis'))
        elif db_type == SourceDatabase.postgres.value:
            reader = Postgres(source_db)
        else:
            raise NotImplementedError(f"Unsupported db_type {db_type}")
        _readers[alias] = reader
    return reader


def get_writer(engine: ClickHouseEngine):
    """
    get writer once
    """
    writer = _writers.get(engine)
    if not writer:
        settings = Global.settings.get('clickhouse')
        if engine == ClickHouseEngine.merge_tree.value:
            writer = ClickHouseMergeTree(settings)
        elif engine == ClickHouseEngine.collapsing_merge_tree:
            writer = ClickHouseCollapsingMergeTree(settings)
        else:
            raise NotImplementedError(f"Unsupported engine {engine}")
        _writers[engine] = writer
    return writer
