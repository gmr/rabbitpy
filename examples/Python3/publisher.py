#!/usr/bin/env python3
import rabbitpy
import logging
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
                                b'Lorem ipsum dolor sit amet, consectetur '
                                b'adipiscing elit.',
                                {'content_type': 'text/plain',
                                 'delivery_mode': 1,
                                 'message_type': 'Lorem ipsum from PYTHON3'})

        # Publish the message
        message.publish(exchange, 'test-routing-key')
