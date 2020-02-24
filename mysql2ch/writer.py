import datetime
import decimal
import json
import logging
import re

import clickhouse_driver

from mysql2ch import DateEncoder

logger = logging.getLogger('mysql2ch.writer')


class ClickHouseWriter:
    def __init__(self, host, port, user, password):
        self.client = clickhouse_driver.Client(host=host, port=port, user=user, password=password)

    def get_ch_column_type(self, db, table):
        column_type_dic = {}
        sql = "select name,type from system.columns where database='{0}' and table='{1}'".format(db, table)
        for d in self.client.execute(sql):
            column_type_dic[d[0]] = d[1]
        return column_type_dic

    def insert_update(self, tmp_data, pk_dict):
        insert_data = []
        exec_sql = {}
        table = tmp_data[0]['table']
        schema = tmp_data[0]['schema']
        column_type = self.get_ch_column_type(schema, table)
        for data in tmp_data:
            for key, value in data['values'].items():
                if value == datetime.datetime(1970, 1, 1, 0, 0):
                    data['values'][key] = datetime.datetime(1970, 1, 2, 14, 1)

                # 处理mysql里面是Null的问题
                if value is None:
                    int_list = ['Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64']
                    if column_type[key] == 'DateTime':
                        data['values'][key] = datetime.datetime(1970, 1, 2, 14, 1)
                    elif column_type[key] == 'Date':
                        data['values'][key] = datetime.date(1970, 1, 2)
                    elif column_type[key] == 'String':
                        data['values'][key] = ''
                    elif column_type[key] in int_list:
                        data['values'][key] = 0

            insert_data.append(data['values'])

        del_sql = self.event_primary_key(schema, table, tmp_data, pk_dict)
        insert_sql = "INSERT INTO {0}.{1} VALUES ".format(schema, table)
        exec_sql['del_sql'] = del_sql
        exec_sql['insert_sql'] = insert_sql
        exec_sql['insert_data'] = insert_data
        exec_sql['db_tb'] = "{0}.{1}".format(schema, table)
        query_sql = del_sql
        query_sql = query_sql.replace('alter table', 'select count(*) from')
        pattern = re.compile(r'\sdelete\s')
        query_sql = re.sub(pattern, ' ', query_sql)
        exec_sql['query_sql'] = query_sql
        return schema, table, exec_sql

    # 根据主键以及没有主键的删除数据处理函数
    def event_primary_key(self, schema, table, tmp_data, pk_dict):
        del_list = []
        last_del = {}
        db_table = "{0}.{1}".format(schema, table)
        primary_key = pk_dict[db_table][0]
        for data in tmp_data:
            for k, v in data['values'].items():
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
        if primary_key:
            for k, v in last_del.items():
                if k not in primary_key:
                    del last_del_tmp[k]
        else:
            message = "delete {0}.{1} 但是mysql里面没有定义主键...".format(schema, table)
            logger.warning(message)
        last_del = last_del_tmp.copy()
        for k, v in last_del_tmp.items():
            last_del[k] = tuple(v)
            nk = k + " in"
            last_del[nk] = last_del.pop(k)
            value_num = len(v)

        replace_max = len(last_del) - 1
        tmp_sql = ''
        for k, v in last_del.items():
            c = str(k) + ' ' + str(v) + " "
            tmp_sql += c
        tmp_sql = tmp_sql.replace(')', ') and', replace_max)

        if value_num == 1:
            del_sql = "alter table {0}.{1} delete where {2}".format(schema, table, tmp_sql)
            del_sql = del_sql.replace(',', '')
            del_sql = del_sql.replace('L', '')
        else:
            del_sql = "alter table {0}.{1} delete where {2}".format(schema, table, tmp_sql)
            del_sql = del_sql.replace('L', '')
        return del_sql

    # 把解析以后的binlog内容拼接成sql入库到ch里面
    def insert_event(self, event, skip_dmls_all, skip_delete_tb_name, pk_dict, only_schemas):
        # 检查mutations是否有失败的(ch后台异步的update和delete变更)
        mutation_list = ['mutation_faild', 'table', 'create_time']
        fail_list = []
        mutation_data = []
        if len(only_schemas) == 1:
            query_sql = "select count(*) from system.mutations where is_done=0 and database in %s" % (
                str(tuple(only_schemas)))
            query_sql = query_sql.replace(",", '')
        else:
            query_sql = "select count(*) from system.mutations where is_done=0 and database in %s" % (
                str(tuple(only_schemas)))
        mutation_sql = "select count(*) as mutation_faild ,concat(database,'.',table)as db,create_time from system.mutations where is_done=0 and database in %s group by db,create_time" % (
            str(tuple(only_schemas)))
        mutations_faild_num = self.client.execute(query_sql)[0][0]
        if mutations_faild_num >= 10:
            fail_data = self.client.execute(mutation_sql)
            for d in fail_data:
                fail_list.append(list(d))
            for d in fail_list:
                tmp = dict(zip(mutation_list, d))
                mutation_data.append(tmp)
            last_data = json.dumps(mutation_data, indent=4, cls=DateEncoder)
            message = "mutations error faild num {0}. delete有失败.请进行检查. 详细信息: {1}".format(mutations_faild_num, last_data)
            logger.error(message)

        # 字段大小写问题的处理
        for data in event:
            for items in data:
                for key, value in items['values'].items():
                    items['values'][key] = items['values'].pop(key)

        # 处理同一条记录update多次的情况
        new_data = []
        for tmp_data in event:
            table = tmp_data[0]['table']
            schema = tmp_data[0]['schema']

            if tmp_data[0]['action'] == 'insert':
                new_data.append(self.keep_new_update(tmp_data, schema, table, pk_dict))
            else:
                new_data.append(tmp_data)

        tmp_data_dic = {}
        event_table = []
        for data in new_data:
            name = '{0}.{1}.{2}'.format(data[0]['schema'], data[0]['table'], data[0]['action'])
            tmp_data_dic[name] = data
            event_table.append(name)

        event_table = list(set(event_table))
        del_ins = self.action_reverse(event_table)

        # 删除多余的insert，并且最后生成需要的格式[[],[]]
        for table_action in del_ins:
            self.del_insert_record(table_action, tmp_data_dic, pk_dict)

        # 生成最后处理好的数据
        last_data = []
        for k, v in tmp_data_dic.items():
            if len(v) != 0:
                last_data.append(v)

        # 排序，执行顺序，delete，insert
        tmp_dict = {}
        i = 0
        for d in last_data:
            tmp_dict[str(str(i))] = d[0]['action_core']
            i = i + 1
        sort_list = sorted(tmp_dict.items(), key=lambda x: x[1])
        new_event = []
        for i in sort_list:
            index = int(i[0])
            new_event.append(last_data[index])

        # 正式把处理完成的数据插入clickhouse
        for tmp_data in new_event:
            if tmp_data[0]['action'] == 'delete':
                table = tmp_data[0]['table']
                schema = tmp_data[0]['schema']
                skip_dml_table_name = "{0}.{1}".format(schema, table)

                del_sql = self.event_primary_key(schema, table, tmp_data, pk_dict)
                logger.debug(f"DELETE 数据删除SQL: {del_sql}")
                try:
                    if 'delete' in skip_dmls_all:
                        return True
                    elif skip_dml_table_name in skip_delete_tb_name:
                        return True
                    else:
                        self.client.execute(del_sql)

                except Exception as error:
                    message = "执行出错SQL:  " + del_sql
                    logger.error(message)
                    logger.error(error)
                    return False

            elif tmp_data[0]['action'] == 'insert':
                schema, table, sql = self.insert_update(tmp_data, pk_dict)
                try:
                    if self.client.execute(sql['query_sql'])[0][0] >= 1:
                        self.client.execute(sql['del_sql'])
                except Exception as error:
                    message = "在插入数据之前删除数据,执行出错SQL:  " + sql['del_sql']
                    logger.error(message)
                    logger.error(error)
                    return False

                message = "INSERT 数据插入SQL: %s %s " % (sql['insert_sql'], str(sql['insert_data']))
                logger.debug(message)
                try:
                    self.client.execute(sql['insert_sql'], sql['insert_data'], types_check=True)
                    num = len(sql['insert_data'])
                    logger.info(f'{schema}.{table}：成功插入 {num} 条数据！')
                except Exception as error:
                    message = "插入数据执行出错SQL:  " + sql['insert_sql'] + str(sql['insert_data'])
                    logger.error(message)
                    logger.error(error)
                    return False
        return True

    # 剔除比较旧的更新，保留最新的更新，否则update的时候数据会多出,因为update已经换成delete+insert。如果不这样处理同一时间update两次就会导致数据多出
    def keep_new_update(self, tmp_data, schema, table, pk_dict):
        db_table = "{0}.{1}".format(schema, table)
        t_dict = {}
        new_update_data = []
        max_time = 0
        same_info = []
        for items in tmp_data:
            sequence_number = items['sequence_number']
            info = items['values'][pk_dict[db_table][0]]

            if info in same_info:
                same_info.append(info)
                if sequence_number > max_time:
                    del t_dict[info]
                    t_dict.setdefault(info, []).append(items)
                    max_time = sequence_number
            else:
                same_info.append(info)
                t_dict.setdefault(info, []).append(items)
                max_time = sequence_number

        for k, v in t_dict.items():
            for d in v:
                new_update_data.append(d)

        del t_dict
        del same_info
        return new_update_data

    # 排序记录,处理insert里面数据多的情况
    def action_reverse(self, event_table):
        del_ins = []
        data_dict = {}
        for items in event_table:
            table = items.split('.')[1]
            schema = items.split('.')[0]
            data_dict.setdefault(table + schema, []).append(items)

        for k, v in data_dict.items():
            if len(v) == 2:
                if v[0].split(".")[2] == 'insert':
                    v.reverse()
                    del_ins.append(v)
                else:
                    del_ins.append(v)
            else:
                del_ins.append(v)

        del data_dict
        return del_ins

    # 删除insert中多余的记录
    def del_insert_record(self, table_action, tmp_data_dic, pk_dict):
        if len(table_action) == 2:
            delete = tmp_data_dic[table_action[0]]
            insert = tmp_data_dic[table_action[1]]
            tb_name = table_action[0].split(".")
            name = "{0}.{1}".format(tb_name[0], tb_name[1])
            pk = pk_dict[name]
            delete_list = []
            for i in delete:
                delete_list.append((i['values'][pk[0]], i['sequence_number']))

            insert2 = []
            for i in insert:
                pk_id = i['values'][pk[0]]
                sequence_number = i['sequence_number']
                insert2.append(i)
                for x, y in delete_list:
                    if pk_id == x and sequence_number < y:
                        insert2.remove(i)
            tmp_data_dic[table_action[1]] = insert2
            del delete_list
            del insert2
