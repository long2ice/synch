import logging
import signal
from signal import Signals

from mysql2ch.factory import Global
from mysql2ch.redis import RedisBroker

logger = logging.getLogger("mysql2ch.consumer")


def consume(args):
    settings = Global.settings
    writer = Global.writer
    reader = Global.reader
    broker = RedisBroker()

    def signal_handler(signum: Signals, handler):
        sig = Signals(signum)
        broker.close()
        logger.info(f"shutdown consumer on {sig.name}")
        exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    schema = args.schema
    skip_error = args.skip_error

    tables = settings.schema_table.get(schema)

    tables_pk = {}
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    event_list = {}
    is_insert = False
    len_event = 0

    logger.info(f"start consumer for {schema} success")
    for msg_id, msg in broker.msgs(
        schema, last_msg_id=args.last_msg_id, block=settings.insert_interval * 1000
    ):
        if not msg_id:
            logger.debug(f"block timeout, len of events:{len_event}")
            if len_event > 0:
                is_insert = True
                alter_table = False
            else:
                continue
        else:
            logger.debug(f"msg_id:{msg_id},msg:{msg}")
            event = msg
            table = event["table"]
            schema = event["schema"]
            action = event["action"]

            if action == "query":
                alter_table = True
                query = event["values"]["query"]
            else:
                alter_table = False
                query = None
                event_list.setdefault(table, []).append(event)
                len_event += 1

        if len_event == settings.insert_num:
            is_insert = True
        if is_insert or alter_table:
            data_dict = {}
            events_num = 0
            for table, items in event_list.items():
                for item in items:
                    action = item["action"]
                    action_core = item["action_core"]
                    data_dict.setdefault(table, {}).setdefault(
                        table + schema + action + action_core, []
                    ).append(item)

            for table, v in data_dict.items():
                tmp_data = []
                for k1, v1 in v.items():
                    events_num += len(v1)
                    tmp_data.append(v1)
                try:
                    result = writer.insert_event(tmp_data, schema, table, tables_pk.get(table))
                    if not result:
                        logger.error("insert event error")
                        if not skip_error:
                            exit()
                except Exception as e:
                    logger.error(f"insert event error,error:{e}")
                    if not skip_error:
                        exit()

            if alter_table:
                try:
                    writer.execute(query)
                    logger.info(f"execute query:{query}")
                except Exception as e:
                    logger.error(f"execute query error,error:{e}")
                    if not skip_error:
                        exit()

            broker.commit(schema)
            event_list = {}
            is_insert = False
            len_event = 0

            logger.info(f"success commit {events_num} events")
