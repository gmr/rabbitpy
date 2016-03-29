#!/usr/bin/env python
import rabbitpy
import datetime
import uuid

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
        tx = rabbitpy.Tx(channel)
        tx.select()

        # Create the message
        message = rabbitpy.Message(channel,
                                   'Lorem ipsum dolor sit amet, consectetur '
                                   'adipiscing elit.',
                                   {'content_type': 'text/plain',
                                    'message_type': 'Lorem ipsum',
                                    'timestamp': datetime.datetime.now(),
                                    'message_id': uuid.uuid1()
                                    })

        # Publish the message
        message.publish(exchange, 'test-routing-key')

        # Commit the message
        tx.commit()

        print('Message published')
