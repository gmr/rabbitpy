#!/usr/bin/env python
import rabbitpy
import logging
import datetime
import uuid
logging.basicConfig(level=logging.DEBUG)

# Use a new connection as a context manager
with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:

    # Use the channel as a context manager
    with conn.channel() as channel:

        # Create the exchange
        exchange = rabbitpy.Exchange(channel, 'example_exchange')
        exchange.declare()

        # Create the queue
        queue = rabbitpy.Queue(channel, 'example')
        queue.declare()

        # Bind the queue
        queue.bind(exchange, 'test-routing-key')

        # Create the msg by passing channel, message and properties (as a dict)
        message = rabbitpy.Message(channel,
                                   'Lorem ipsum dolor sit amet, consectetur '
                                   'adipiscing elit.',
                                   {'content_type': 'text/plain',
                                    'delivery_mode': 1,
                                    'message_type': 'Lorem ipsum',
                                    'timestamp': datetime.datetime.now(),
                                    'message_id': uuid.uuid1()})

        # Publish the message
        message.publish(exchange, 'test-routing-key')
