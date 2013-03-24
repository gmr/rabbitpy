#!/usr/bin/env python
import rmqid

with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:

        # Create the exchange
        exchange = rmqid.Exchange(channel, 'test_exchange')
        exchange.declare()

        # Create the queue
        queue = rmqid.Queue(channel, 'test_queue')
        queue.declare()

        # Bind the queue
        queue.bind(exchange, 'test-routing-key')

        # Create the message
        message = rmqid.Message(channel,
                                'Lorem ipsum dolor sit amet, consectetur '
                                'adipiscing elit.',
                                {'content_type': 'text/plain',
                                 'type': 'Lorem ipsum'})

        # Publish the message
        message.publish(exchange, 'test-routing-key')

        print 'Message published'
