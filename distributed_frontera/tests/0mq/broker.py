# -*- coding: utf-8 -*-
import zmq
from binascii import hexlify

# Prepare our context and sockets
context = zmq.Context()
input = context.socket(zmq.XSUB)
output = context.socket(zmq.XPUB)
input.bind("tcp://127.0.0.1:5559")
output.bind("tcp://127.0.0.1:5560")

# Initialize poll set
poller = zmq.Poller()
poller.register(input, zmq.POLLIN)
poller.register(output, zmq.POLLIN)

# Switch messages between sockets
while True:
    socks = dict(poller.poll())

    if socks.get(input) == zmq.POLLIN:
        message = input.recv_multipart(copy=True)
        output.send_multipart(message)
        print "I: ", message

    if socks.get(output) == zmq.POLLIN:
        message = output.recv_multipart(copy=True)
        input.send_multipart(message)
        print "O: ", message