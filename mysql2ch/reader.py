import datetime
import logging
import time

import MySQLdb
from MySQLdb.cursors import DictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent

logger = logging.getLogger('mysql2ch.reader')


class MysqlReader:
    only_events = (DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent)

    def __init__(self, host, port, user, password):
        self.password = password
        self.user = user
        self.port = int(port)
        self.host = host
        self.conn = MySQLdb.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            connect_timeout=5,
            cursorclass=DictCursor,
            charset='utf8'
        )

    def execute(self, sql):
        logger.debug(sql)
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def get_primary_key(self, db, table):
        """
        get pk
        :param db:
        :param table:
        :return:
        """
        pri_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}' and COLUMN_KEY='PRI'"
        result = self.execute(pri_sql)
        return result[0]['COLUMN_NAME']

    def check_table_exists(self, db, table):
        sql = f"select count(*) as count from information_schema.tables where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}'"
        result = self.execute(sql)
        return result[0]['count']

    # binglog接收函数
    def binlog_reading(self, server_id, only_tables, only_schemas, log_file, log_pos):
        logger.info('start sync %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        logger.info(f'mysql binlog: {log_file}:{log_pos}')
        stream = BinLogStreamReader(connection_settings=dict(
            host=self.host, port=self.port, user=self.user, passwd=self.password
        ), resume_stream=True, blocking=True,
            server_id=int(server_id), only_tables=only_tables, only_schemas=only_schemas,
            only_events=self.only_events, log_file=log_file, log_pos=log_pos,
            fail_on_table_metadata_unavailable=True, slave_heartbeat=10)
        for binlog_event in stream:
            for row in binlog_event.rows:
                event = {}
                if isinstance(binlog_event, WriteRowsEvent):
                    event['action'] = 'insert'
                    event['values'] = row['values']
                    event['event_unixtime'] = int(time.time() * 10 ** 6)
                    event['action_core'] = '2'

                elif isinstance(binlog_event, UpdateRowsEvent):
                    event['action'] = 'insert'
                    event['values'] = row['after_values']
                    event['event_unixtime'] = int(time.time() * 10 ** 6)
                    event['action_core'] = '2'

                elif isinstance(binlog_event, DeleteRowsEvent):
                    event['action'] = 'delete'
                    event['values'] = row['values']
                    event['event_unixtime'] = int(time.time() * 10 ** 6)
                    event['action_core'] = '1'

                yield binlog_event.schema, binlog_event.table, event, stream.log_file, stream.log_pos
