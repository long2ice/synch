from typing import Dict

from synch.enums import ClickHouseEngine, SourceDatabase
from synch.factory import Global
from synch.reader import Reader
from synch.writer import ClickHouse
from synch.writer.collapsing_merge_tree import ClickHouseCollapsingMergeTree
from synch.writer.merge_tree import ClickHouseMergeTree

_readers: Dict[str, Reader] = {}
_writers: Dict[str, ClickHouse] = {}


def get_reader(alias: str):
    """
    get reader once
    """
    r = _readers.get(alias)
    if not r:
        source_db = Global.settings.get_source_db(alias)
        if not source_db:
            raise Exception(f"Can't find alias {alias} in config.")
        db_type = source_db.get("db_type")
        if db_type == SourceDatabase.mysql.value:
            from synch.reader.mysql import Mysql
            r = Mysql(source_db, Global.settings.get('redis'))
        elif db_type == SourceDatabase.postgres.value:
            from synch.reader.postgres import Postgres
            r = Postgres(source_db)
        else:
            raise NotImplementedError(f"Unsupported db_type {db_type}")
        _readers[alias] = r
    return r


def get_writer(engine: ClickHouseEngine):
    """
    get writer once
    """
    w = _writers.get(engine)
    if not w:
        settings = Global.settings.get('clickhouse')
        if engine == ClickHouseEngine.merge_tree.value:
            w = ClickHouseMergeTree(settings)
        elif engine == ClickHouseEngine.collapsing_merge_tree:
            w = ClickHouseCollapsingMergeTree(settings)
        else:
            raise NotImplementedError(f"Unsupported engine {engine}")
        _writers[engine] = w
    return w
