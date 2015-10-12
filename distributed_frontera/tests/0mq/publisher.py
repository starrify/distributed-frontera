# -*- coding: utf-8 -*-
import zmq
from random import randrange

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://127.0.0.1:5559")

while True:
    zipcode = randrange(1, 100000)
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    socket.send_string("%i %i %i" % (zipcode, temperature, relhumidity))