import logging
import signal
from signal import Signals

from synch.factory import Global

logger = logging.getLogger("synch.replication.consumer")

is_stop = False
is_insert = False


def consume(args):
    settings = Global.settings
    writer = Global.writer
    reader = Global.reader
    broker = Global.broker
    global is_stop
    global is_insert

    def signal_handler(signum: Signals, handler):
        global is_stop
        global is_insert
        sig = Signals(signum)
        is_stop = True
        is_insert = True
        logger.info(f"shutdown consumer on {sig.name}, wait seconds to finish...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    schema = args.schema
    skip_error = args.skip_error

    tables = settings.schema_table.get(schema)
    # try etl full
    if settings.auto_full_etl:
        reader.etl_full(writer, schema, tables)

    tables_pk = {}
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    event_list = {}
    len_event = 0

    logger.info(f"start consumer for {schema} success")
    for msg_id, msg in broker.msgs(
        schema, last_msg_id=args.last_msg_id, block=settings.insert_interval * 1000
    ):
        if not msg_id:
            if len_event > 0:
                is_insert = True
                alter_table = False
            else:
                if is_stop:
                    logger.info("finish success, bye!")
                    broker.close()
                    exit()
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
                pk = tables_pk.get(table)
                if isinstance(pk, tuple):
                    pk = pk[0]
                for k1, v1 in v.items():
                    events_num += len(v1)
                    tmp_data.append(v1)
                if skip_error:
                    try:
                        result = writer.insert_event(tmp_data, schema, table, pk)
                        if not result:
                            logger.error("insert event error")
                    except Exception as e:
                        logger.error(f"insert event error,error:{e}")
                else:
                    writer.insert_event(tmp_data, schema, table, tables_pk.get(table))
            if alter_table:
                try:
                    writer.execute(query)
                    logger.info(f"execute query:{query}")
                except Exception as e:
                    logger.error(f"execute query error,error:{e}")
                    if not skip_error:
                        exit()

            broker.commit(schema)
            logger.info(f"success commit {events_num} events")
            event_list = {}
            is_insert = False
            len_event = 0
            if is_stop:
                logger.info("finish success,bye!")
                broker.close()
                exit()
