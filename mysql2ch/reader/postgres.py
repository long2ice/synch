import logging
from typing import Tuple, Union

import psycopg2
import psycopg2.errors
from psycopg2._psycopg import ReplicationMessage
from psycopg2.extras import DictCursor, PhysicalReplicationConnection, ReplicationCursor

from mysql2ch.broker import Broker
from mysql2ch.reader import Reader
from mysql2ch.settings import Settings

logger = logging.getLogger("mysql2ch.reader.postgres")


class Postgres(Reader):
    _conn = {}

    def __init__(self, settings: Settings):
        super().__init__(settings)
        params = dict(host=settings.postgres_host, port=settings.postgres_port, user=settings.postgres_user,
                      password=settings.postgres_password)
        for database in settings.schema_table.keys():
            replication_conn = psycopg2.connect(
                **params, database=database, connection_factory=PhysicalReplicationConnection
            )
            conn = psycopg2.connect(**params, database=database, cursor_factory=DictCursor)
            self._conn[database] = {
                'repl_cursor': replication_conn.cursor(),
                'cursor': conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            }

    def _get_cursor(self, database: str, repl: bool = False):
        if repl:
            return self._conn.get(database).get('repl_cursor')
        else:
            return self._conn.get(database).get('cursor')

    def execute(self, database: str, sql, args=None):
        logger.debug(sql)
        cursor = self._get_cursor(database)
        cursor.execute(sql, args)
        return cursor.fetchall()

    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        sql = f"""SELECT a.attname
FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
    AND a.attnum = ANY (i.indkey)
WHERE i.indrelid = '{db}.public.{table}'::regclass 
AND i.indisprimary;"""
        ret = self.execute(db, sql)
        return ret[0]

    def _consumer(self, msg: ReplicationMessage):
        print(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def start_sync(self, broker: Broker):
        for database in self.settings.schema_table:
            cursor = self._get_cursor(database, True)  # type:ReplicationCursor
            cursor.start_replication(slot_type=psycopg2.extras.REPLICATION_PHYSICAL, start_lsn='0/4F5A2DA8',
                                     status_interval=1)
            cursor.consume_stream(self._consumer)
