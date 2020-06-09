import json
import logging
from typing import Optional

from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient, KafkaProducer
from kafka.admin import NewPartitions

from mysql2ch.brokers import Broker
from mysql2ch.common import JsonEncoder

logger = logging.getLogger('mysql2ch.brokers.kafka')


class KafkaBroker(Broker):
    producer: Optional[KafkaProducer] = None
    consumer: Optional[KafkaConsumer] = None

    def _partitioner(self, key_bytes, all_partitions, available_partitions):
        """
        custom partitioner depend on settings
        :param key_bytes:
        :param all_partitions:
        :param available_partitions:
        :return:
        """
        key = key_bytes.decode()
        partition = self.settings.schema_table.get(key).get("kafka_partition")
        return all_partitions[partition]

    def send(self, schema: str, msg: dict):
        """
        send msg for special schema
        :param schema:
        :param msg:
        :return:
        """
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka_server,
                value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
                key_serializer=lambda x: x.encode(),
                partitioner=self._partitioner,
            )
            self._init_partitions()
        self.producer.send(
            topic=self.settings.kafka_topic, value=msg, key=schema,
        )

    def msgs(self, schema: str, msg_from: str, offset: Optional[int] = None):
        """
        consume msgs from broker
        :param msg_from:
        :param schema:
        :param offset:
        :return:
        """
        if not self.consumer:
            schema_table = self.settings.schema_table.get(schema)
            partition = schema_table.get("kafka_partition")
            topic = self.settings.kafka_topic
            topic_partition = TopicPartition(topic, partition)
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.settings.kafka_server,
                value_deserializer=lambda x: json.loads(x, object_hook=self._object_hook),
                key_deserializer=lambda x: x.decode() if x else None,
                enable_auto_commit=False,
                group_id=schema,
                auto_offset_reset=msg_from,
            )
            self.consumer.assign([topic_partition])
            if offset:
                self.consumer.seek(topic_partition, offset)
        for msg in self.consumer:
            yield msg.value

    def commit(self):
        self.consumer.commit()

    def _init_partitions(self):
        client = KafkaAdminClient(bootstrap_servers=self.settings.kafka_server, )
        try:
            client.create_partitions(
                topic_partitions={
                    self.settings.kafka_topic: NewPartitions(total_count=len(self.settings.schema_table.keys()))
                }
            )
        except Exception as e:
            logger.warning(f"init_partitions error:{e}")
