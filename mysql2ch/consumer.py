import json
import logging

from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from mysql2ch import settings
from . import writer, reader
from .common import object_hook

logger = logging.getLogger('mysql2ch.consumer')


def consume(args):
    schema = args.schema
    tables = args.tables
    skip_error = args.skip_error
    assert schema in settings.SCHEMAS, f'schema {schema} must in settings.SCHEMAS'
    topic = settings.KAFKA_TOPIC
    tables_pk = {}
    partitions = []
    for table in tables.split(','):
        assert table in settings.TABLES, f'table {table} must in settings.TABLES'

        partition = settings.PARTITIONS.get(f'{schema}.{table}')
        tp = TopicPartition(topic, partition)
        partitions.append(tp)
        tables_pk[table] = reader.get_primary_key(schema, table)

    group_id = f'{schema}.{tables}'
    consumer = KafkaConsumer(
        bootstrap_servers=settings.KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
        key_deserializer=lambda x: x.decode() if x else None,
        enable_auto_commit=False,
        group_id=group_id,
        auto_offset_reset='earliest',
    )
    consumer.assign(partitions)

    event_list = {}
    is_insert = False
    last_time = 0
    len_event = 0
    logger.info(f'success consume topic:{topic},partitions:{partitions},schema:{schema},tables:{tables}')

    for msg in consumer:  # type:ConsumerRecord
        logger.debug(f'kafka msg:{msg}')
        event = msg.value
        event_unixtime = event['event_unixtime'] / 10 ** 6
        table = event['table']
        schema = event['schema']
        event_list.setdefault(table, []).append(event)
        len_event += 1

        if last_time == 0:
            last_time = event_unixtime

        if len_event == settings.INSERT_NUMS:
            is_insert = True
        else:
            if event_unixtime - last_time >= settings.INSERT_INTERVAL > 0:
                is_insert = True
        if is_insert:
            data_dict = {}
            events_num = 0
            for table, items in event_list.items():
                for item in items:
                    action = item['action']
                    action_core = item['action_core']
                    data_dict.setdefault(table, {}).setdefault(table + schema + action + action_core, []).append(item)
            for table, v in data_dict.items():
                tmp_data = []
                for k1, v1 in v.items():
                    events_num += len(v1)
                    tmp_data.append(v1)
                result = writer.insert_event(tmp_data, schema, table, tables_pk.get(table))
                if not result:
                    logger.error('insert event error!')
                    if skip_error:
                        continue
                    exit()

            consumer.commit()
            logger.info(f'commit success {events_num} events!')
            event_list = {}
            is_insert = False
            len_event = last_time = 0
