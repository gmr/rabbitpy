rmqid
=====
A pure python, minimalistic and pythonic BSD Licensed AMQP/RabbitMQ library.

Requirements
------------
  - pamqp - https://github.com/pika/pamqp

Example Publisher
-----------------
    import datetime
    import logging
    import rmqid
    import uuid

    logging.basicConfig(level=logging.DEBUG)

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

            # Send the message
            message.publish(exchange, 'test-routing-key')

Example Consumer
----------------
    import logging
    import rmqid

    logging.basicConfig(level=logging.DEBUG)

    with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rmqid.Queue(channel, 'test_queue')

            try:
                # Consume the message
                for message in queue.consume():
                    logging.debug('Message body: %s', message.body)
                    logging.debug('Message sent at: %s',
                                  message.properties.timestamp.isoformat())
                    logging.debug('Delivery tag: %i', message.method.delivery_tag)

                    # Ack the delivery
                    message.ack()

            # Handle CTRL-C and send a Basic.Cancel, canceling the consumer
            except KeyboardInterrupt:
                queue.cancel()
