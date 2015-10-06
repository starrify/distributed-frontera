# -*- coding: utf-8 -*-
from abc import abstractmethod, abstractproperty, ABCMeta

class BaseStreamConsumer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_messages(self, timeout=0.1, count=1):
        """
        :return: generator with raw messages
        """
        raise NotImplementedError


class BaseStreamProducer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, key, *messages):
        """
        Sending messages to stream.
        :param key: str key used for partitioning, None for non-keyed channels
        :param *messages: encoded message(s)
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self):
        raise NotImplementedError


class BaseSpiderLogStream(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def producer(self):
        """
        Using FingerprintPartitioner (because of state cache in Strategy Workers)
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError

    @abstractmethod
    def consumer(self, partition_id, type):
        """
        Messages consumed by all known groups can be freed
        :param partition_id: int
        :param type: consumer type, can be either "sw" or "db"
        :return: BaseStreamConsumer instance assigned to given partition_id
        """
        raise NotImplementedError


class BaseUpdateScoreStream(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def consumer(self):
        """
        :return: BaseStreamConsumer instance
        """
        raise NotImplementedError

    @abstractmethod
    def producer(self):
        """
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError


class BaseSpiderFeedStream(object):
    @abstractmethod
    def consumer(self, partition_id):
        """
        :param partition_id:
        :return: BaseStreamConsumer instance assigned to given partition_id
        """
        raise NotImplementedError

    @abstractmethod
    def producer(self):
        """
        Using Crc32NamePartitioner
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError

    @abstractmethod
    def available_partitions(self):
        """
        :return: list of ints
        """
        raise NotImplementedError


class BaseMessageBus(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def update_score(self):
        """
        :return: instance of UpdateScoreStream
        """
        raise NotImplementedError

    @abstractproperty
    def spider_log(self):
        """
        :return: instance of SpiderLogStream
        """
        raise NotImplementedError

    @abstractproperty
    def spider_feed(self):
        """
        :return: instance of SpiderFeedStream
        """
        raise NotImplementedError