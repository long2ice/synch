from typing import Union, Tuple

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection, DictCursor

from mysql2ch.reader import Reader


class Postgres(Reader):
    def __init__(self, host: str = '127.0.0.1', port: int = 5432, user: str = 'postgres', password: str = '', **extra):
        super().__init__(host, port, user, password, **extra)
        params = dict(host=host, port=port, user=user, password=password)
        self.replication_conn = psycopg2.connect(**params,
                                                 connection_factory=PhysicalReplicationConnection)
        self.replication_cursor = self.replication_conn.cursor()
        self.conn = psycopg2.connect(**params, cursor_factory=DictCursor)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def get_binlog_pos(self) -> Tuple[str, str]:
        pass

    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        sql = """select pg_constraint.conname as pk_name, pg_attribute.attname as colname, pg_type.typname as typename
from pg_constraint
         inner join pg_class
                    on pg_constraint.conrelid = pg_class.oid
         inner join pg_attribute on pg_attribute.attrelid = pg_class.oid
    and pg_attribute.attnum = pg_constraint.conkey[1]
         inner join pg_type on pg_type.oid = pg_attribute.atttypid
where pg_class.relname = 'test'
  and pg_constraint.contype = 'p'"""
        ret = self.execute(sql)
        return ret[0][1]

    def start_replication(self):
        pass
