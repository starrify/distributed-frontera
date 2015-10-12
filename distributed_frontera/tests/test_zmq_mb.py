# -*- coding: utf-8 -*-

from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
from time import sleep

def test_spider_log_producer():
    mb = MessageBus(None)
    producer = mb.spider_log().producer()
    while True:
        producer.send(sha1('helloworld.com'), 'http://helloworld.com/way/to/the/sun')
        producer.send(sha1('oups1.com'), 'http://way.to.the.sun')
        print "."
        sleep(1)


def test_spider_log_consumer():
    mb = MessageBus(None)
    consumer = mb.spider_log().consumer(partition_id=1, type=None)
    while True:
        for m in consumer.get_messages(count=10):
            print m
