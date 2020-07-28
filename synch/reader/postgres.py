import functools
import json
import logging
import threading
import time
from signal import Signals
from typing import Callable, Tuple, Union

import psycopg2
import psycopg2.errors
from psycopg2._psycopg import ReplicationMessage
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor, LogicalReplicationConnection, ReplicationCursor

from synch.broker import Broker
from synch.reader import Reader
from synch.settings import Settings

logger = logging.getLogger("synch.reader.postgres")


class Postgres(Reader):
    _repl_conn = {}
    lock = threading.Lock()
    lsn = None

    def __init__(self, alias):
        super().__init__(alias)
        source_db = Settings.get_source_db(alias)
        params = dict(
            host=source_db.get("host"),
            port=source_db.get("port"),
            user=source_db.get("user"),
            password=source_db.get("password"),
        )
        self.insert_interval = Settings.insert_interval()
        self.skip_dmls = source_db.get("skip_dmls") or []
        self.skip_update_tables = source_db.get("skip_update_tables") or []
        self.skip_delete_tables = source_db.get("skip_delete_tables") or []
        self.conn = psycopg2.connect(**params, cursor_factory=DictCursor)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()
        for database in source_db.get("databases"):
            database_name = database.get("database")
            replication_conn = psycopg2.connect(
                **params, database=database_name, connection_factory=LogicalReplicationConnection
            )
            self._repl_conn[database_name] = {
                "cursor": replication_conn.cursor(),
            }

    def execute(self, sql, args=None):
        try:
            return super(Postgres, self).execute(sql, args)
        except psycopg2.ProgrammingError:
            pass

    def _get_repl_cursor(self, database: str):
        return self._repl_conn.get(database).get("cursor")

    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        sql = f"""SELECT a.attname
FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
    AND a.attnum = ANY (i.indkey)
WHERE i.indrelid = '{db}.public.{table}'::regclass AND i.indisprimary;"""
        ret = self.execute(sql)
        return ret[0][0]

    def _consumer(self, broker: Broker, database: str, msg: ReplicationMessage):
        payload = json.loads(msg.payload)
        change = payload.get("change")
        if not change:
            return
        change = change[0]
        kind = change.get("kind")
        table = change.get("table")
        columnnames = change.get("columnnames")
        columnvalues = change.get("columnvalues")
        oldkeys = change.get("oldkeys")
        skip_dml_table_name = f"{database}.{table}"
        delete_event = event = None
        if kind == "update":
            values = dict(zip(columnnames, columnvalues))
            if (
                "update" not in self.skip_dmls
                and skip_dml_table_name not in self.skip_update_tables
            ):
                delete_event = {
                    "table": table,
                    "schema": database,
                    "action": "delete",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_seq": 1,
                }
                event = {
                    "table": table,
                    "schema": database,
                    "action": "insert",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_seq": 2,
                }
        elif kind == "delete":
            values = dict(zip(oldkeys.get("keynames"), oldkeys.get("keyvalues")))
            if (
                "delete" not in self.skip_dmls
                and skip_dml_table_name not in self.skip_delete_tables
            ):
                event = {
                    "table": table,
                    "schema": database,
                    "action": "delete",
                    "values": values,
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_seq": 1,
                }
        elif kind == "insert":
            values = dict(zip(columnnames, columnvalues))
            event = {
                "table": table,
                "schema": database,
                "action": "insert",
                "values": values,
                "event_unixtime": int(time.time() * 10 ** 6),
                "action_seq": 2,
            }
        else:
            return
        event["values"] = self.deep_decode_dict(event["values"])
        if delete_event:
            broker.send(msg=delete_event, schema=database)
        if event:
            broker.send(msg=event, schema=database)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        logger.debug(f"send to queue success: key:{database},event:{event}")
        logger.debug(f"success flush lsn: {msg.data_start}")

        with self.lock:
            self.lsn = msg.data_start
            self.after_send(database, table)

    def signal_handler(self, signum: Signals, handler: Callable):
        sig = Signals(signum)
        logger.info(f"shutdown producer on {sig.name}, current lsn: {self.lsn}")
        exit()

    def _run(
        self, broker, database,
    ):
        logger.info(f"start producer success for database: {database}")
        cursor = self._get_repl_cursor(database)  # type:ReplicationCursor
        try:
            cursor.create_replication_slot("synch", output_plugin="wal2json")
        except psycopg2.errors.DuplicateObject:
            pass
        cursor.start_replication(slot_name="synch", decode=True, status_interval=1)
        cursor.consume_stream(functools.partial(self._consumer, broker, database))

    def start_sync(self, broker: Broker):
        for database in self.source_db.get("databases"):
            t = threading.Thread(target=self._run, args=(broker, database.get("database"),))
            t.setDaemon(True)
            t.start()
            t.join()

    def get_source_select_sql(self, schema: str, table: str, sign_column=None):
        select = "*"
        if sign_column:
            select += f", toInt8(1) as {sign_column}"
        return f"SELECT {select} FROM jdbc('postgresql://{self.host}:{self.port}/{schema}?user={self.user}&password={self.password}', '{table}')"
