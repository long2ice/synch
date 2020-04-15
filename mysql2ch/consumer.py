import json
import logging
import time

from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from . import writer, reader
import settings
from .common import object_hook

logger = logging.getLogger('mysql2ch.consumer')


def consume(args):
    schema = args.schema
    table = args.table
    skip_error = args.skip_error
    assert schema in settings.SCHEMAS, 'schema must in settings.SCHEMAS'
    assert table in settings.TABLES, 'table must in settings.TABLES'
    group_id = f'{schema}.{table}'
    consumer = KafkaConsumer(
        bootstrap_servers=settings.KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
        key_deserializer=lambda x: x.decode() if x else None,
        enable_auto_commit=False,
        group_id=group_id,
        auto_offset_reset='earliest',
    )
    topic = settings.KAFKA_TOPIC
    partition = settings.PARTITIONS.get(group_id)
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])

    event_list = []
    is_insert = False
    last_time = 0
    logger.info(f'success consume topic:{topic},partition:{partition},schema:{schema},table:{table}')

    pk = reader.get_primary_key(schema, table)

    for msg in consumer:  # type:ConsumerRecord
        logger.debug(f'kafka msg:{msg}')
        event = msg.value
        event_unixtime = event['event_unixtime'] / 10 ** 6
        event_list.append(event)
        len_event = len(event_list)
        if last_time == 0:
            last_time = event_unixtime

        if len_event == settings.INSERT_NUMS:
            is_insert = True
        else:
            if event_unixtime - last_time >= settings.INSERT_INTERVAL > 0:
                is_insert = True
        if is_insert:
            data_dict = {}
            tmp_data = []
            for items in event_list:
                action = items['action']
                action_core = items['action_core']
                data_dict.setdefault(table + schema + action + action_core, []).append(
                    dict(items, schema=schema, table=table))
            for k, v in data_dict.items():
                tmp_data.append(v)
            result = writer.insert_event(tmp_data, schema, table, pk)
            if result or (not result and skip_error):
                event_list = []
                is_insert = False
                last_time = 0
                consumer.commit()
                logger.info(f'commit success {len_event} events!')
            else:
                logger.error('insert event error!')
                exit()
