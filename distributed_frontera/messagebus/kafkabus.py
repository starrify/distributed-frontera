# -*- coding: utf-8 -*-
from distributed_frontera.messagebus.base import BaseMessageBus, BaseSpiderLogStream, BaseSpiderFeedStream, \
    BaseStreamConsumer, BaseUpdateScoreStream

from kafka import KafkaClient, SimpleConsumer, KeyedProducer, SimpleProducer
from kafka.common import BrokerResponseError, MessageSizeTooLargeError
from kafka.protocol import CODEC_SNAPPY

from distributed_frontera.worker.partitioner import FingerprintPartitioner, Crc32NamePartitioner
from distributed_frontera.worker.offsets import Fetcher
from logging import getLogger, StreamHandler
from time import sleep

logger = getLogger("distributed_frontera.messagebus.kafka")


class Consumer(BaseStreamConsumer):
    """
    Used in DB and SW worker. SW consumes per partition.
    """
    def __init__(self, conn, topic, group, partition_id):
        self._conn = conn
        self._group = group
        self._topic = topic
        self._partition_ids = [partition_id] if partition_id else None

        self._cons = None
        self._connect_consumer()

    def _connect_consumer(self):
        if self._cons is None:
            try:
                self._cons = SimpleConsumer(
                    self._conn,
                    self._group,
                    self._topic,
                    partitions=self._partition_ids,
                    buffer_size=1048576,
                    max_buffer_size=10485760)
            except BrokerResponseError:
                self._cons = None
                logger.warning("Could not connect consumer to Kafka server")
                return False
        return True

    def get_messages(self, timeout=0.1, count=1):
        if not self._connect_consumer():
            yield
        while True:
            try:
                success = False
                for offmsg in self._cons.get_messages(
                        count,
                        timeout=timeout):
                    success = True
                    try:
                        yield offmsg.message.value
                    except ValueError:
                        logger.warning(
                            "Could not decode {0} message: {1}".format(
                                self._topic,
                                offmsg.message.value))
                if not success:
                    logger.warning(
                        "Timeout ({0} seconds) while trying to get {1} requests".format(
                            timeout,
                            count)
                    )
                break
            except Exception, err:
                logger.warning("Error %s" % err)
                break


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self._conn = messagebus.conn
        self._topic_done = messagebus.topic_done
        self._partition_id = messagebus.spider_partition_id
        self._db_group = messagebus.general_group
        self._sw_group = messagebus.sw_group
        self._cons = None
        self._connect_producer()

    def _connect_producer(self):
        if self._prod is None:
            try:
                self._prod = KeyedProducer(self._conn, partitioner=FingerprintPartitioner, codec=CODEC_SNAPPY)
            except BrokerResponseError:
                self._prod = None
                logger.warning("Could not connect producer to Kafka server")
                return False
        return True

    def put(self, message, key, fail_wait_time=1.0, max_tries=5):
        success = False
        if self._connect_producer():
            n_tries = 0
            while not success and n_tries < max_tries:
                try:
                    self._prod.send_messages(self._topic_done, key, message)
                    success = True
                except MessageSizeTooLargeError, e:
                    logger.error(str(e))
                    logger.debug("Message: %s" % message)
                    break
                except BrokerResponseError:
                    n_tries += 1
                    logger.warning(
                        "Could not send message. Try {0}/{1}".format(
                            n_tries, max_tries)
                    )
                    sleep(fail_wait_time)
        return success

    def consumer(self, partition_id, type):
        group = self._sw_group if type == 'sw' else self._db_group
        return Consumer(self._conn, self._topic_done, group, partition_id)

    def flush(self):
        if self._prod is not None:
            self._prod.stop()


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self._conn = messagebus.conn
        self._general_group = messagebus.general_group
        self._topic = messagebus.topic_todo
        self._max_next_requests = messagebus.max_next_requests
        self._offset_fetcher = Fetcher(self._conn, self._topic, self._general_group)
        self._producer = KeyedProducer(self._conn, partitioner=Crc32NamePartitioner, codec=CODEC_SNAPPY)

    def consumer(self, partition_id):
        return Consumer(self._conn, self._topic, self._general_group, partition_id)

    def available_partitions(self):
        partitions = []
        lags = self._offset_fetcher.get()
        for partition, lag in lags.iteritems():
            if lag < self._max_next_requests:
                partitions.append(partition)
        return partitions

    def put(self, message, key):
        self._producer.send_messages(self._topic, key, message)


class UpdateScoreStream(BaseUpdateScoreStream):
    def __init__(self, messagebus):
        self._scoring_consumer = SimpleConsumer(messagebus.conn,
                                       messagebus.general_group,
                                       messagebus.topic_scoring,
                                       buffer_size=262144,
                                       max_buffer_size=1048576)
        self._producer = SimpleProducer(messagebus.conn, codec=CODEC_SNAPPY)
        self._topic = messagebus.topic_scoring

    def get(self):
        return self._scoring_consumer.get_message()

    def put(self, message):
        self._producer.send_messages(self._topic, message)


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        server = settings.get('KAFKA_LOCATION')
        self.topic_todo = settings.get('OUTGOING_TOPIC', "frontier-todo")
        self.topic_done = settings.get('INCOMING_TOPIC', "frontier-done")
        self.topic_scoring = settings.get('SCORING_TOPIC')
        self.general_group = settings.get('FRONTIER_GROUP', "general")
        self.sw_group = settings.get('SCORING_GROUP', "strategy-workers")
        self.spider_partition_id = settings.get('SPIDER_PARTITION_ID')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS

        self.conn = KafkaClient(server)

        logger = getLogger("kafka")
        handler = StreamHandler()
        logger.addHandler(handler)

    def spider_log(self):
        return SpiderLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

    def update_score(self):
        return UpdateScoreStream(self)