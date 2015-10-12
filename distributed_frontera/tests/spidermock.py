# -*- coding: utf-8 -*-
import sys
from time import sleep
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1


def main():
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    producer = sl.producer()

    while True:
        producer.send(sha1('helloworld.com'), 'http://helloworld.com/way/to/the/sun')
        producer.send(sha1('oups.com'), 'http://way.to.the.sun')
        sys.stdout.write(".")
        sleep(1)


if __name__ == '__main__':
    main()