# -*- coding: utf-8 -*-
import sys
from time import sleep
from distributed_frontera.messagebus.zeromq import MessageBus


def main():
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    consumer = sl.consumer(partition_id=None, type='sw')
    while True:
        for m in consumer.get_messages(timeout=1.0):
            print m
        sys.stdout.write('.')


if __name__ == '__main__':
    main()