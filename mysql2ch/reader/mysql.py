import datetime
import logging
import time
from typing import Union, Tuple

import MySQLdb
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from mysql2ch.convert import SqlConvert
from mysql2ch.reader import Reader

logger = logging.getLogger("mysql2ch.reader")


class Mysql(Reader):
    only_events = (DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent)

    def __init__(self, host="127.0.0.1", port=3306, user="root", password=None, **extra):
        super().__init__(host, port, user, password, **extra)
        self.conn = MySQLdb.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            **self.extra
        )
        self.cursor = self.conn.cursor()

    def get_binlog_pos(self) -> Tuple[str, str]:
        """
        get binlog pos from master
        """
        sql = "show master status"
        result = self.execute(sql)[0]
        return result.get("File"), result.get("Position")

    def get_primary_key(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        """
        get pk
        :param db:
        :param table:
        :return:
        """
        pri_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}' and COLUMN_KEY='PRI'"
        result = self.execute(pri_sql)
        if not result:
            return None
        if len(result) > 1:
            return tuple(map(lambda x: x.get("COLUMN_NAME"), result))
        return result[0]["COLUMN_NAME"]

    def binlog_reading(
            self,
            server_id,
            only_tables,
            only_schemas,
            log_file,
            log_pos,
            skip_dmls,
            skip_update_tables,
            skip_delete_tables,
    ):
        logger.info("start sync at %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logger.info(f"mysql binlog: {log_file}:{log_pos}")
        stream = BinLogStreamReader(
            connection_settings=dict(
                host=self.host, port=self.port, user=self.user, passwd=self.password
            ),
            resume_stream=True,
            blocking=True,
            server_id=server_id,
            only_tables=only_tables,
            only_schemas=only_schemas,
            only_events=self.only_events,
            log_file=log_file,
            log_pos=log_pos,
            fail_on_table_metadata_unavailable=True,
            slave_heartbeat=10,
        )
        for binlog_event in stream:
            if isinstance(binlog_event, QueryEvent):
                schema = binlog_event.schema.decode()
                query = binlog_event.query.lower()
                if "alter" not in query:
                    continue
                convent_sql = SqlConvert.to_clickhouse(schema, query)
                if not convent_sql:
                    continue
                event = {
                    "table": None,
                    "schema": schema,
                    "action": "query",
                    "values": {"query": convent_sql},
                    "event_unixtime": int(time.time() * 10 ** 6),
                    "action_core": "0",
                }
                yield schema, None, event, stream.log_file, stream.log_pos
            else:
                schema = binlog_event.schema
                table = binlog_event.table
                skip_dml_table_name = f"{schema}.{table}"
                for row in binlog_event.rows:
                    if isinstance(binlog_event, WriteRowsEvent):
                        event = {
                            "table": table,
                            "schema": schema,
                            "action": "insert",
                            "values": self.convert_values(row["values"]),
                            "event_unixtime": int(time.time() * 10 ** 6),
                            "action_core": "2",
                        }

                    elif isinstance(binlog_event, UpdateRowsEvent):
                        if "update" in skip_dmls or skip_dml_table_name in skip_update_tables:
                            continue
                        delete_event = {
                            "table": table,
                            "schema": schema,
                            "action": "delete",
                            "values": self.convert_values(row["before_values"]),
                            "event_unixtime": int(time.time() * 10 ** 6),
                            "action_core": "1",
                        }
                        yield binlog_event.schema, binlog_event.table, delete_event, stream.log_file, stream.log_pos
                        event = {
                            "table": table,
                            "schema": schema,
                            "action": "insert",
                            "values": self.convert_values(row["after_values"]),
                            "event_unixtime": int(time.time() * 10 ** 6),
                            "action_core": "2",
                        }

                    elif isinstance(binlog_event, DeleteRowsEvent):
                        if "delete" in skip_dmls or skip_dml_table_name in skip_delete_tables:
                            continue
                        event = {
                            "table": table,
                            "schema": schema,
                            "action": "delete",
                            "values": self.convert_values(row["values"]),
                            "event_unixtime": int(time.time() * 10 ** 6),
                            "action_core": "1",
                        }
                    else:
                        return
                    yield binlog_event.schema, binlog_event.table, event, stream.log_file, stream.log_pos
