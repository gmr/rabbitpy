#!/usr/bin/env python3
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:

        # Create the exchange
        exchange = rabbitpy.Exchange(channel, 'example_exchange')
        exchange.declare()

        # Create the queue
        queue = rabbitpy.Queue(channel, 'example')
        queue.declare()

        # Bind the queue
        queue.bind(exchange, 'test-routing-key')

        # Create and start the transaction
        tx = rabbitpy.TX(channel)
        tx.select()

        # Create the message
        message = rabbitpy.Message(channel,
                                b'Lorem ipsum dolor sit amet, consectetur '
                                b'adipiscing elit.',
                                {'content_type': 'text/plain',
                                 'message_type': 'Lorem ipsum from PYTHON3'})

        # Publish the message
        message.publish(exchange, 'test-routing-key')

        # Commit the message
        tx.commit()
