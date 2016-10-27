#!/usr/bin/env python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')
        while len(queue) > 0:
            message = queue.get()
            message.pprint(True)
            message.ack()
            print('There are {} more messages in the queue'.format(len(queue)))