import json
import logging
import time

from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

import settings
from mysql2ch import writer, reader

logger = logging.getLogger('mysql2ch.consumer')


def consume(args):
    schema = args.schema
    table = args.table
    assert schema in settings.SCHEMAS, 'schema must in settings.SCHEMAS'
    assert table in settings.TABLES, 'table must in settings.TABLES'
    consumer = KafkaConsumer(
        bootstrap_servers=settings.KAFKA_SERVER,
        value_deserializer=json.loads,
        key_deserializer=lambda x: x.decode(),
        enable_auto_commit=False,
        group_id=f'{schema}.{table}'
    )
    consumer.assign(
        [TopicPartition(topic=settings.KAFKA_TOPIC, partition=settings.PARTITIONS.get(f'{schema}.{table}'))])
    event_list = []
    pk = reader.get_primary_key(schema, table)
    for msg in consumer:  # type:ConsumerRecord
        event = msg.value
        event_list.append(event)
        if len(event_list) == settings.INSERT_NUMS or (
                (int(time.time() * 10 ** 6) - event_list[0][
                    'event_unixtime']) / 10 ** 6 >= settings.INSERT_INTERVAL > 0):
            data_dict = {}
            tmp_data = []
            for items in event_list:
                action = items['action']
                action_core = items['action_core']
                data_dict.setdefault(table + schema + action + action_core, []).append(items)
            for k, v in data_dict.items():
                tmp_data.append(v)
            writer.insert_event(tmp_data, settings.SKIP_TYPE, settings.SKIP_DELETE_TB_NAME, schema, table, pk)
            event_list = []
            consumer.commit()
