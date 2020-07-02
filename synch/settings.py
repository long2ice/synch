import functools
from typing import Dict, List

import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class Settings:
    _config: Dict

    def __init__(self, file_path: str):
        with open(file_path, "r") as f:
            self._config = yaml.load(f, Loader)

    @functools.cached_property
    def debug(self):
        return self.get('core', "debug")

    @functools.cached_property
    def insert_interval(self):
        return self.get('core', "insert_interval")

    @functools.cached_property
    def broker_type(self):
        return self.get('core', "broker_type")

    @functools.cached_property
    def insert_num(self):
        return self.get('core', "insert_num")

    @functools.cached_property
    def auto_full_etl(self):
        return self.get('core', "auto_full_etl")

    @functools.lru_cache()
    def get_source_db(self, alias: str) -> Dict:
        return next(filter(lambda x: x.get("alias") == alias, self.get("source_dbs")))

    @functools.lru_cache()
    def get_source_db_database(self, alias: str, database: str) -> Dict:
        source_db = self.get_source_db(alias)
        return next(filter(lambda x: x.get("database") == database, source_db.get("databases")))

    @functools.lru_cache()
    def get_source_db_database_tables_name(self, alias: str, database: str) -> List[str]:
        return list(
            map(lambda x: x.get("table"), self.get_source_db_database_tables(alias, database))
        )

    @functools.lru_cache()
    def get_source_db_database_tables(self, alias: str, database: str) -> List[Dict]:
        """
        get table list
        """
        source_db_database = self.get_source_db_database(alias, database)
        return source_db_database.get("tables")

    @functools.lru_cache()
    def get_source_db_database_tables_by_tables_name(
            self, alias: str, database: str, tables: List[str]
    ):
        source_db_database_tables = self.get_source_db_database_tables(alias, database)
        return list(filter(lambda x: x.get("table") in tables, source_db_database_tables))

    @functools.lru_cache()
    def get_source_db_database_table(self, alias: str, database: str, table: str) -> Dict:
        """
        get table dict
        """
        return next(
            filter(
                lambda x: x.get("table") == table,
                self.get_source_db_database_tables(alias, database),
            )
        )

    @functools.lru_cache()
    def get(self, *args):
        """
        get config item
        """
        c = self._config
        for arg in args:
            c = c.get(arg)
        return c
