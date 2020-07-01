import abc
import json
import logging
import signal
from signal import Signals
from typing import Callable, Dict, List

import clickhouse_driver

from synch.common import JsonEncoder
from synch.settings import Settings

logger = logging.getLogger("synch.replication.clickhouse")


class ClickHouse:
    len_event = 0
    is_stop = False
    is_insert = False
    event_list = {}

    def __init__(self, settings: Settings, broker):
        self.settings = settings
        self.broker = broker
        self._client = clickhouse_driver.Client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def table_exists(self, schema: str, table: str):
        sql = f"select count(*)from system.tables where database = '{schema}' and name = '{table}'"
        ret = self.execute(sql)[0][0]
        return bool(ret)

    def execute(self, sql, params=None, *args, **kwargs):
        log_sql = sql
        if params:
            log_sql = f"{sql}{params}"
        logger.debug(log_sql)
        return self._client.execute(sql, params=params, *args, **kwargs)

    def fix_table_column_type(self, reader, database, table):
        """
        fix table column type in full etl
        :return:
        """
        sql = f"select COLUMN_NAME, COLUMN_TYPE,IS_NULLABLE from information_schema.COLUMNS where TABLE_NAME = '{table}' and COLUMN_TYPE like '%decimal%'and TABLE_SCHEMA = '{database}'"
        cursor = reader.conn.cursor()
        cursor.execute(sql)
        logger.debug(sql)
        ret = cursor.fetchall()
        cursor.close()
        for item in ret:
            column_name = item.get("COLUMN_NAME")
            is_nullable = item.get("IS_NULLABLE")
            column_type = item.get("COLUMN_TYPE").title()
            if is_nullable:
                fix_sql = f"alter table {database}.{table} modify column {column_name} Nullable({column_type})"
            else:
                fix_sql = (
                    f"alter table {database}.{table} modify column {column_name} {column_type}"
                )
            self.execute(fix_sql)

    def check_mutations(self, schema: str):
        """
        检查mutations是否有失败的(ch后台异步的update和delete变更)
        """
        mutation_list = ["mutation_failed", "table", "create_time"]
        fail_list = []
        mutation_data = []
        query_sql = (
            f"select count(*) from system.mutations where is_done=0 and database = '{schema}'"
        )
        mutation_sql = f"select count(*) as mutation_faild ,concat(database,'.',table)as db,create_time from system.mutations where is_done=0 and database = '{schema}' group by db,create_time"
        mutations_failed_num = self.execute(query_sql)[0][0]
        if mutations_failed_num >= 10:
            fail_data = self.execute(mutation_sql)
            for d in fail_data:
                fail_list.append(list(d))
            for d in fail_list:
                tmp = dict(zip(mutation_list, d))
                mutation_data.append(tmp)
            last_data = json.dumps(mutation_data, indent=4, cls=JsonEncoder)
            message = "mutations error failed num {0}. delete error please check: {1}".format(
                mutations_failed_num, last_data
            )
            logger.error(message)

    def insert_events(self, schema: str, table: str, insert_data: List[Dict]):
        insert_sql = "insert into {0}.{1} values ".format(schema, table)
        self.execute(insert_sql, list(map(lambda x: x.get("values"), insert_data)))

    def signal_handler(self, signum: Signals, handler: Callable):
        sig = Signals(signum)
        if self.len_event == 0:
            logger.info(f"shutdown consumer on {sig.name} success")
            exit()
        else:
            logger.info(
                f"shutdown consumer on {sig.name}, wait {self.settings.insert_interval} seconds at most to finish {self.len_event} events..."
            )
            self.is_stop = True
            self.is_insert = True

    def finish(self):
        logger.info("finish success, bye!")
        self.broker.close()
        exit()

    def after_insert(self, schema: str, events_num: int):
        self.broker.commit(schema)
        logger.info(f"success commit {events_num} events")
        self.event_list = {}
        self.is_insert = False
        self.len_event = 0
        if self.is_stop:
            self.finish()

    def alter_table(self, query: str, skip_error: bool):
        """
        alter table
        """
        if skip_error:
            try:
                self.execute(query)
                logger.info(f"execute query: {query}")
            except Exception as e:
                logger.error(f"execute query error, e:{e}")
        else:
            self.execute(query)
            logger.info(f"execute ddl query: {query}")

    @abc.abstractmethod
    def start_consume(self, schema: str, tables_pk: Dict, last_msg_id, skip_error: bool):
        raise NotImplementedError
