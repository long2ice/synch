import functools
from typing import Dict, List

import yaml


class Settings:
    _config: Dict

    @classmethod
    def init(cls, file_path: str):
        with open(file_path, "r") as f:
            cls._config = yaml.safe_load(f)

    @classmethod
    def debug(cls):
        return cls.get("core", "debug")

    @classmethod
    def monitoring(cls):
        return cls.get("core", "monitoring")

    @classmethod
    def insert_interval(cls):
        return cls.get("core", "insert_interval")

    @classmethod
    def insert_num(cls):
        return cls.get("core", "insert_num")

    @classmethod
    @functools.lru_cache()
    def get_source_db(cls, alias: str) -> Dict:
        return next(filter(lambda x: x.get("alias") == alias, cls.get("source_dbs")))

    @classmethod
    @functools.lru_cache()
    def is_cluster(cls):
        return True if cls.get("clickhouse").get("cluster_name") else False

    @classmethod
    @functools.lru_cache()
    def cluster_name(cls):
        return cls.get("clickhouse").get("cluster_name")

    @classmethod
    @functools.lru_cache()
    def get_source_db_database(cls, alias: str, database: str) -> Dict:
        source_db = cls.get_source_db(alias)
        return next(filter(lambda x: x.get("database") == database, source_db.get("databases")))

    @classmethod
    @functools.lru_cache()
    def get_source_db_database_tables_name(cls, alias: str, database: str) -> List[str]:
        return list(
            map(lambda x: x.get("table"), cls.get_source_db_database_tables(alias, database))
        )

    @classmethod
    @functools.lru_cache()
    def get_source_db_database_tables(cls, alias: str, database: str) -> List[Dict]:
        """
        get table list
        """
        source_db_database = cls.get_source_db_database(alias, database)
        return source_db_database.get("tables")

    @classmethod
    @functools.lru_cache()
    def get_source_db_database_tables_by_tables_name(
        cls, alias: str, database: str, tables: List[str]
    ):
        source_db_database_tables = cls.get_source_db_database_tables(alias, database)
        return list(filter(lambda x: x.get("table") in tables, source_db_database_tables))

    @classmethod
    @functools.lru_cache()
    def get_source_db_database_tables_dict(cls, alias: str, database: str) -> Dict:
        ret = {}
        for table in cls.get_source_db_database_tables(alias, database):
            ret[table.get("table")] = table
        return ret

    @classmethod
    @functools.lru_cache()
    def get_source_db_database_table(cls, alias: str, database: str, table: str) -> Dict:
        """
        get table dict
        """
        return next(
            filter(
                lambda x: x.get("table") == table,
                cls.get_source_db_database_tables(alias, database),
            )
        )

    @classmethod
    @functools.lru_cache()
    def get(cls, *args):
        """
        get config item
        """
        c = cls._config
        for arg in args:
            c = c.get(arg)
        return c
