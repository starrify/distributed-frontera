# -*- coding: utf-8 -*-
from base import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, BaseSpiderFeedStream, \
    BaseUpdateScoreStream
from distributed_frontera.worker.partitioner import FingerprintPartitioner, Crc32NamePartitioner
import zmq
from time import time, sleep
from struct import pack
import six


class Consumer(BaseStreamConsumer):
    def __init__(self, context, location, partition_id):
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(location)

        filter = pack('>B', partition_id) if partition_id is not None else ''
        self.subscriber.setsockopt(zmq.SUBSCRIBE, filter)

    def get_messages(self, timeout=0.1, count=1):
        started = time()
        while count:
            try:
                msg = self.subscriber.recv_multipart(copy=True, flags=zmq.NOBLOCK)
            except zmq.Again:
                sleep(0.01)
                if time() - started > timeout:
                    break
            else:
                yield msg[1]
                count -= 1


class Producer(object):
    def __init__(self, context, location):
        self.sender = context.socket(zmq.PUB)
        self.sender.bind(location)

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")

        partition = self.partitioner.partition(key)
        for msg in messages:
            self.sender.send_multipart([pack(">B", partition), msg])

    def flush(self):
        pass


class SpiderLogProducer(Producer):
    def __init__(self, context, location, partitions):
        super(SpiderLogProducer, self).__init__(context, location)
        self.partitioner = FingerprintPartitioner(partitions)


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.location = messagebus.spider_log_location
        self.partitions = messagebus.spider_log_partitions

    def producer(self):
        return SpiderLogProducer(self.context, self.location, self.partitions)

    def consumer(self, partition_id, type):
        return Consumer(self.context, self.location, partition_id)


class UpdateScoreProducer(Producer):
    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        for msg in messages:
            self.sender.send_multipart(['', msg])


class UpdateScoreStream(BaseUpdateScoreStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.location = messagebus.update_score_location

    def consumer(self):
        return Consumer(self.context, self.location, partition_id=None)

    def producer(self):
        return UpdateScoreProducer(self.context, self.location)


class SpiderFeedProducer(Producer):
    def __init__(self, context, location, partitions):
        super(SpiderFeedProducer, self).__init__(context, location)
        self.partitioner = Crc32NamePartitioner(partitions)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.location = messagebus.spider_feed_location
        self.partitions = messagebus.spider_feed_partitions

    def consumer(self, partition_id):
        return Consumer(self.context, self.location, partition_id)

    def producer(self):
        return SpiderFeedProducer(self.context, self.location, self.partitions)

    def available_partitions(self):
        return self.partitions


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.context = zmq.Context()

        # FIXME: Options!
        self.spider_log_location = "tcp://127.0.0.1:5551"
        self.spider_log_partitions = [i for i in range(1)]
        self.update_score_location = "tcp://127.0.0.1:5552"
        self.spider_feed_location = "tcp://127.0.0.1:5553"
        self.spider_feed_partitions = [i for i in range(1)]

    def spider_log(self):
        return SpiderLogStream(self)

    def update_score(self):
        return UpdateScoreStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)