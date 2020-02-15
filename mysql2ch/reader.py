import datetime
import logging
import time

import MySQLdb
from MySQLdb.cursors import DictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent

from mysql2ch.config import Config

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

    def close_db(self):
        self.conn.close()

    def slave_status(self):
        cursor = self.conn.cursor()
        cursor.execute('show slave status')
        result = cursor.fetchall()
        cursor.close()

        if result:
            return result[0]

    def sql_query(self, sql):
        cursor = self.conn.cursor()
        count = cursor.execute(sql)
        if count == 0:
            result = 0
        else:
            result = 1
        return result

    def get_primary_key(self, db, table):
        # 获取自增主键信息
        primary_key = []
        pri_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}' and COLUMN_KEY='PRI'"
        uni_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}' and COLUMN_KEY='UNI'"
        if self.sql_query(pri_sql):
            sql = pri_sql
        elif self.sql_query(uni_sql):
            sql = uni_sql
        else:
            sql = pri_sql
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()

        if result:
            for data in result:
                primary_key.append(data['COLUMN_NAME'])
        return primary_key

    def check_table_exists(self, db, table):
        sql = f"select count(*) as count from information_schema.tables where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}'"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result[0]['count']

    # binglog接收函数
    def binlog_reading(self, server_id, only_tables, only_schemas, log_file, log_pos, insert_nums,
                       interval):
        event_list = []
        sequence = 0
        logger.info('开始同步数据时间 %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        logger.info(f'数据库binlog：{log_file}:{log_pos}')
        pk_dict = {}
        for schema in only_schemas:
            for table in only_tables:
                pk = self.get_primary_key(schema, table)
                name = '{0}.{1}'.format(schema, table)
                if pk:
                    pk_dict[name] = pk
                    logger.info(f'开始同步: {name}')
                else:
                    if self.check_table_exists(schema, table):
                        logger.error(f'要同步的表: {name} 不存在主键或者唯一键，程序退出....')
                        exit(1)
        stream = BinLogStreamReader(connection_settings=dict(
            host=self.host, port=self.port, user=self.user, passwd=self.password
        ), resume_stream=True, blocking=True,
            server_id=int(server_id), only_tables=only_tables, only_schemas=only_schemas,
            only_events=self.only_events, log_file=log_file, log_pos=log_pos,
            fail_on_table_metadata_unavailable=True, slave_heartbeat=10)
        try:
            for binlog_event in stream:
                for row in binlog_event.rows:
                    sequence += 1
                    event = {'schema': binlog_event.schema, 'table': binlog_event.table, 'sequence_number': sequence}
                    if isinstance(binlog_event, WriteRowsEvent):
                        event['action'] = 'insert'
                        event['values'] = row['values']
                        event['event_unixtime'] = int(time.time())
                        event['action_core'] = '2'

                    elif isinstance(binlog_event, UpdateRowsEvent):
                        event['action'] = 'insert'
                        event['values'] = row['after_values']
                        event['event_unixtime'] = int(time.time())
                        event['action_core'] = '2'

                    elif isinstance(binlog_event, DeleteRowsEvent):
                        event['action'] = 'delete'
                        event['values'] = row['values']
                        event['event_unixtime'] = int(time.time())
                        event['action_core'] = '1'

                    event_list.append(event)

                    if len(event_list) == insert_nums or (
                            int(time.time()) - event_list[0]['event_unixtime'] >= interval > 0):
                        repl_status = self.slave_status()
                        log_file = stream.log_file
                        log_pos = stream.log_pos
                        if repl_status:
                            Config.pos_handler.set_log_pos_master(
                                repl_status['Master_Host'],
                                repl_status['Master_Port'],
                                repl_status['Relay_Master_Log_File'],
                                repl_status['Exec_Master_Log_Pos']
                            )

                        data_dict = {}
                        tmp_data = []
                        for items in event_list:
                            table = items['table']
                            schema = items['schema']
                            action = items['action']
                            action_core = items['action_core']
                            data_dict.setdefault(table + schema + action + action_core, []).append(items)
                        for k, v in data_dict.items():
                            tmp_data.append(v)
                        event_list = []
                        sequence = 0
                        yield tmp_data, pk_dict, log_file, log_pos
        except KeyboardInterrupt:
            log_file, log_pos = Config.pos_handler.get_log_pos()
            message = '同步程序退出,当前同步位置 {0}:{1}'.format(log_file, log_pos)
            logger.info(message)
