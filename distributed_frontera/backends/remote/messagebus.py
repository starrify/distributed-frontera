# -*- coding: utf-8 -*-
from frontera import Backend, Settings
from frontera.core import OverusedBuffer
from distributed_frontera.messagebus.kafkabus import MessageBus
from codecs.msgpack import Encoder, Decoder


class MessageBusBackend(Backend):
    def __init__(self, manager):
        self._manager = manager
        settings = manager.settings

        self.mb = MessageBus(settings)
        self._encoder = Encoder(manager.request_model)
        self._decoder = Decoder(manager.request_model, manager.response_model)
        self.spider_log_producer = self.mb.spider_log().producer()
        spider_feed = self.mb.spider_feed()
        self.consumer = spider_feed.consumer(partition_id=settings.get('SPIDER_PARTITION_ID'))
        self._get_timeout = float(settings.get('KAFKA_GET_TIMEOUT', 5.0))

        self._buffer = OverusedBuffer(self._get_next_requests,
                                      manager.logger.manager.debug)

    @classmethod
    def from_manager(clas, manager):
        return clas(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.spider_log_producer.flush()

    def add_seeds(self, seeds):
        self.spider_log_producer.send(seeds[0].meta['fingerprint'], self._encoder.encode_add_seeds(seeds))

    def page_crawled(self, response, links):
        self.spider_log_producer.send(response.meta['fingerprint'], self._encoder.encode_page_crawled(response, links))

    def request_error(self, page, error):
        self.spider_log_producer.send(page.meta['fingerprint'], self._encoder.encode_request_error(page, error))

    def _get_next_requests(self, max_n_requests, **kwargs):
        requests = []
        for encoded in self.consumer.get_messages(count=max_n_requests, timeout=self._get_timeout):
            try:
                request = self._decoder.decode_request(encoded)
                requests.append(request)
            except ValueError:
                self._manager.logger.backend.warning("Could not message: {0}".format(encoded))
        return requests

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._buffer.get_next_requests(max_n_requests, **kwargs)