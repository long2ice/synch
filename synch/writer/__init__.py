import logging
from typing import Dict, List

import clickhouse_driver

logger = logging.getLogger("synch.replication.clickhouse")


class ClickHouse:
    len_event = 0
    is_stop = False
    is_insert = False
    event_list = {}

    def __init__(self, settings: Dict):
        self.settings = settings
        self._client = clickhouse_driver.Client(
            host=settings.get("host"),
            port=settings.get("port"),
            user=settings.get("user"),
            password=settings.get("password") or "",
        )

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

    def insert_events(self, schema: str, table: str, insert_data: List[Dict]):
        insert_sql = "insert into {0}.{1} values ".format(schema, table)
        self.execute(insert_sql, list(map(lambda x: x.get("values"), insert_data)))

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
