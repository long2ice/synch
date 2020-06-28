import datetime
import decimal
import json
import logging
import re

import clickhouse_driver

from synch.common import JsonEncoder

logger = logging.getLogger("synch.replication.clickhouse")


class ClickHouseWriter:
    def __init__(self, host="127.0.0.1", port="9000", user="default", password=""):
        self._client = clickhouse_driver.Client(host=host, port=port, user=user, password=password)

    def table_exists(self, schema: str, table: str):
        sql = f"select count(*)from system.tables where database = '{schema}' and name = '{table}'"
        ret = self.execute(sql)[0][0]
        return bool(ret)

    def execute(self, sql, params=None, *args, **kwargs):
        log_sql = sql
        if params:
            log_sql = f"{sql} {params}"
        logger.debug(log_sql)
        return self._client.execute(sql, params=params, *args, **kwargs)

    def fix_table_column_type(self, reader, database, table):
        """
        fix table column type in full etl
        :return:
        """
        sql = f"select COLUMN_NAME, COLUMN_TYPE from information_schema.COLUMNS where TABLE_NAME = '{table}' and COLUMN_TYPE like '%decimal%'and TABLE_SCHEMA = '{database}'"
        cursor = reader.conn.cursor()
        cursor.execute(sql)
        logger.debug(sql)
        ret = cursor.fetchall()
        cursor.close()
        for item in ret:
            column_name = item.get("COLUMN_NAME")
            column_type = item.get("COLUMN_TYPE").title()
            fix_sql = f"alter table {database}.{table} modify column {column_name} {column_type}"
            self.execute(fix_sql)

    def insert_update(self, tmp_data, schema, table, pk):
        insert_data = []
        exec_sql = {}
        for data in tmp_data:
            insert_data.append(data["values"])

        del_sql = self.event_primary_key(schema, table, tmp_data, pk)
        insert_sql = "INSERT INTO {0}.{1} VALUES ".format(schema, table)
        exec_sql["del_sql"] = del_sql
        exec_sql["insert_sql"] = insert_sql
        exec_sql["insert_data"] = insert_data
        exec_sql["db_tb"] = "{0}.{1}".format(schema, table)
        query_sql = del_sql
        query_sql = query_sql.replace("alter table", "select count(*) from")
        pattern = re.compile(r"\sdelete\s")
        query_sql = re.sub(pattern, " ", query_sql)
        exec_sql["query_sql"] = query_sql
        return schema, table, exec_sql

    # 根据主键以及没有主键的删除数据处理函数
    def event_primary_key(self, schema, table, tmp_data, pk):
        del_list = []
        last_del = {}
        for data in tmp_data:
            for k, v in data["values"].items():
                data_dict = {}
                if type(v) == datetime.datetime:
                    data_dict[k] = str(v)
                elif type(v) == datetime.date:
                    data_dict[k] = str(v)
                elif type(v) == decimal.Decimal:
                    data_dict[k] = str(v)
                else:
                    data_dict[k] = v

                del_list.append(data_dict)

        for i in del_list:
            for k, v in i.items():
                last_del.setdefault(k, []).append(v)
        last_del_tmp = last_del.copy()
        if pk:
            for k, v in last_del.items():
                if k not in pk:
                    del last_del_tmp[k]
        else:
            message = "delete {0}.{1} but no pk...".format(schema, table)
            logger.warning(message)
        last_del = last_del_tmp.copy()
        for k, v in last_del_tmp.items():
            last_del[k] = tuple(v)
            nk = k + " in"
            last_del[nk] = last_del.pop(k)
            value_num = len(v)

        replace_max = len(last_del) - 1
        tmp_sql = ""
        for k, v in last_del.items():
            c = str(k) + " " + str(v) + " "
            tmp_sql += c
        tmp_sql = tmp_sql.replace(")", ") and", replace_max)

        if value_num == 1:
            del_sql = "alter table {0}.{1} delete where {2}".format(schema, table, tmp_sql)
            del_sql = del_sql.replace(",", "")
        else:
            del_sql = "alter table {0}.{1} delete where {2}".format(schema, table, tmp_sql)
        return del_sql

    # 把解析以后的binlog内容拼接成sql入库到ch里面
    def insert_event(self, tmp_data, schema, table, pk):
        # 检查mutations是否有失败的(ch后台异步的update和delete变更)
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

        # 处理同一条记录update多次的情况
        new_data = []
        for data in tmp_data:
            if data[0]["action"] == "insert":
                new_data.append(self.keep_new_update(data, pk))
            else:
                new_data.append(data)

        tmp_data_dic = {}
        event_table = []
        for data in new_data:
            name = "{0}.{1}.{2}".format(schema, table, data[0]["action"])
            tmp_data_dic[name] = data
            event_table.append(name)

        event_table = list(set(event_table))
        del_ins = self.action_reverse(event_table)

        # 删除多余的insert，并且最后生成需要的格式[[],[]]
        for table_action in del_ins:
            self.del_insert_record(table_action, tmp_data_dic, pk)

        # 生成最后处理好的数据
        last_data = []
        for k, v in tmp_data_dic.items():
            if len(v) != 0:
                last_data.append(v)

        # 排序，执行顺序，delete，insert
        tmp_dict = {}
        i = 0
        for d in last_data:
            tmp_dict[str(str(i))] = d[0]["action_core"]
            i = i + 1
        sort_list = sorted(tmp_dict.items(), key=lambda x: x[1])
        new_event = []
        for i in sort_list:
            index = int(i[0])
            new_event.append(last_data[index])

        # 正式把处理完成的数据插入clickhouse
        for tmp_data in new_event:
            if tmp_data[0]["action"] == "delete":
                del_sql = self.event_primary_key(schema, table, tmp_data, pk)
                try:
                    self.execute(del_sql)

                except Exception as error:
                    message = f"exec sql error,sql:{del_sql},error:{error}"
                    logger.error(message)
                    return False
                logger.info(f"{schema}.{table}：success delete {len(tmp_data)} rows")

            elif tmp_data[0]["action"] == "insert":
                schema, table, sql = self.insert_update(tmp_data, schema, table, pk)
                try:
                    if self.execute(sql["query_sql"])[0][0] >= 1:
                        self.execute(sql["del_sql"])
                except Exception as error:
                    message = f"delete before insert error,sql: {sql['del_sql']},error:{error}"
                    logger.error(message)
                    return False
                self.execute(sql["insert_sql"], sql["insert_data"], types_check=True)
                num = len(sql["insert_data"])
                logger.info(f"{schema}.{table}：success insert {num} rows")
        return True

    # 剔除比较旧的更新，保留最新的更新，否则update的时候数据会多出,因为update已经换成delete+insert。如果不这样处理同一时间update两次就会导致数据多出
    def keep_new_update(self, tmp_data, pk):
        t_dict = {}
        new_update_data = []
        max_time = 0
        same_info = []
        for items in tmp_data:
            event_unixtime = items["event_unixtime"]
            info = items["values"][pk]

            if info in same_info:
                same_info.append(info)
                if event_unixtime > max_time:
                    del t_dict[info]
                    t_dict.setdefault(info, []).append(items)
                    max_time = event_unixtime
            else:
                same_info.append(info)
                t_dict.setdefault(info, []).append(items)
                max_time = event_unixtime

        for k, v in t_dict.items():
            for d in v:
                new_update_data.append(d)
        return new_update_data

    # 排序记录,处理insert里面数据多的情况
    def action_reverse(self, event_table):
        del_ins = []
        data_dict = {}
        for items in event_table:
            table = items.split(".")[1]
            schema = items.split(".")[0]
            data_dict.setdefault(table + schema, []).append(items)

        for k, v in data_dict.items():
            if len(v) == 2:
                if v[0].split(".")[2] == "insert":
                    v.reverse()
                    del_ins.append(v)
                else:
                    del_ins.append(v)
            else:
                del_ins.append(v)
        return del_ins

    # 删除insert中多余的记录
    def del_insert_record(self, table_action, tmp_data_dic, pk):
        if len(table_action) == 2:
            delete = tmp_data_dic[table_action[0]]
            insert = tmp_data_dic[table_action[1]]
            delete_list = []
            for i in delete:
                delete_list.append((i["values"][pk], i["event_unixtime"]))

            insert2 = []
            for i in insert:
                pk_id = i["values"][pk]
                event_unixtime = i["event_unixtime"]
                insert2.append(i)
                for x, y in delete_list:
                    if pk_id == x and event_unixtime < y:
                        insert2.remove(i)
            tmp_data_dic[table_action[1]] = insert2
