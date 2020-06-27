import functools
import json
import logging
import time
from typing import Tuple, Union

import psycopg2
import psycopg2.errors
from psycopg2._psycopg import ReplicationMessage
from psycopg2.extras import DictCursor, LogicalReplicationConnection, ReplicationCursor

from mysql2ch.broker import Broker
from mysql2ch.reader import Reader
from mysql2ch.settings import Settings

logger = logging.getLogger("mysql2ch.reader.postgres")


class Postgres(Reader):
    _repl_conn = {}
    count = last_time = 0

    def __init__(self, settings: Settings):
        super().__init__(settings)
        params = dict(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
        self.conn = psycopg2.connect(**params, cursor_factory=DictCursor)
        self.cursor = self.conn.cursor()
        for database in settings.schema_table.keys():
            replication_conn = psycopg2.connect(
                **params, database=database, connection_factory=LogicalReplicationConnection
            )
            self._repl_conn[database] = {
                "cursor": replication_conn.cursor(),
            }

    def _get_repl_cursor(self, database: str):
        return self._repl_conn.get(database).get("cursor")

    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        sql = f"""SELECT a.attname
FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
    AND a.attnum = ANY (i.indkey)
WHERE i.indrelid = '{db}.public.{table}'::regclass 
AND i.indisprimary;"""
        ret = self.execute(sql)
        return ret[0]

    def _consumer(self, broker: Broker, database: str, msg: ReplicationMessage):
        payload = json.loads(msg.payload)
        print(payload)
        change = payload.get("change")
        if not change:
            return
        change = change[0]
        kind = change.get("kind")
        table = change.get("table")
        columnnames = change.get("columnnames")
        columnvalues = change.get("columnvalues")
        skip_dml_table_name = f"{database}.{table}"
        values = dict(zip(columnnames, columnvalues))
        if kind == "update":
            if (
                "update" not in self.settings.skip_dmls
                and skip_dml_table_name not in self.settings.skip_update_tables
            ):
                delete_event = {
                    "table": table,
                    "schema": database,
                    "action": "delete",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_core": "1",
                }
                event = {
                    "table": table,
                    "schema": database,
                    "action": "insert",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_core": "2",
                }
        elif kind == "delete":
            if (
                "delete" not in self.settings.skip_dmls
                and skip_dml_table_name not in self.settings.skip_delete_tables
            ):
                event = {
                    "table": table,
                    "schema": database,
                    "action": "delete",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_core": "1",
                }
        elif kind == "insert":
            event = {
                "table": table,
                "schema": database,
                "action": "insert",
                "values": values,
                "event_unixtime": int(time.time() * 10 ** 6),
                "action_core": "2",
            }
        else:
            return
        broker.send(msg=event, schema=database)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        logger.debug(f"send to queue success: key:{database},event:{event}")
        logger.debug(f"success flush lsn:{msg.data_start}")

        now = int(time.time())
        self.count += 1

        if self.last_time == 0:
            self.last_time = now
        if now - self.last_time >= self.settings.insert_interval:
            logger.info(
                f"success send {self.count} events in {self.settings.insert_interval} seconds"
            )
            self.last_time = self.count = 0

    def start_sync(self, broker: Broker):
        for database in self.settings.schema_table:
            cursor = self._get_repl_cursor(database)  # type:ReplicationCursor
            try:
                cursor.create_replication_slot("mysql2ch", output_plugin="wal2json")
            except psycopg2.errors.DuplicateObject:
                pass
            cursor.start_replication(slot_name="mysql2ch", decode=True, status_interval=1)
            cursor.consume_stream(functools.partial(self._consumer, broker, database))
