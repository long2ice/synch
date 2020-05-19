import json
import logging

from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from .common import Global, object_hook

logger = logging.getLogger("mysql2ch.consumer")


def consume(args):
    settings = Global.settings
    writer = Global.writer
    reader = Global.reader

    schema = args.schema
    skip_error = args.skip_error
    auto_offset_reset = args.auto_offset_reset
    topic = settings.kafka_topic
    tables_pk = {}
    schema_table = settings.schema_table.get(schema)
    tables = schema_table.get("tables")

    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_server,
        value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
        key_deserializer=lambda x: x.decode() if x else None,
        enable_auto_commit=False,
        group_id=schema,
        auto_offset_reset=auto_offset_reset,
    )
    partition = schema_table.get("kafka_partition")
    consumer.assign([TopicPartition(topic, partition)])

    event_list = {}
    is_insert = False
    last_time = 0
    len_event = 0
    logger.info(
        f"success consume topic:{topic},partitions:{partition},schema:{schema},tables:{tables}"
    )

    for msg in consumer:  # type:ConsumerRecord
        logger.debug(f"kafka msg:{msg}")
        event = msg.value
        event_unixtime = event["event_unixtime"] / 10 ** 6
        table = event["table"]
        schema = event["schema"]
        action = event["action"]

        if action == "query":
            do_query = True
            query = event["values"]["query"]
        else:
            do_query = False
            query = None
            event_list.setdefault(table, []).append(event)
            len_event += 1

        if last_time == 0:
            last_time = event_unixtime

        if len_event == settings.insert_num:
            is_insert = True
        else:
            if event_unixtime - last_time >= settings.insert_interval > 0:
                is_insert = True
        if is_insert or do_query:
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
                        logger.error("insert event error!")
                        if not skip_error:
                            exit()
                except Exception as e:
                    logger.error(f"insert event error!,error:{e}")
                    if not skip_error:
                        exit()
            if do_query:
                try:
                    logger.info(f"execute query:{query}")
                    writer.execute(query)
                except Exception as e:
                    logger.error(f"execute query error!,error:{e}")
                    if not skip_error:
                        exit()
            consumer.commit()
            logger.info(f"commit success {events_num} events!")
            event_list = {}
            is_insert = False
            len_event = last_time = 0
