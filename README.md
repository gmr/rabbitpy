rmqid
=====
A pure python, minimalistic and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6, 2.7 and 3.3.

Version
-------
The current released alpha version is 0.2.0

Installation
------------
rmqid may be installed via the Python package index with the tool of your choice. I prefer pip:

    pip install rmqid

Requirements
------------
  - pamqp - https://github.com/pika/pamqp

Python3 Caveats
---------------
 - Message bodies must use the bytes data type while most other values are strings.

Example Publisher
-----------------
In this example, messages are being published while using the connection and
channel as context managers.

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

            # Send the message
            message.publish(exchange, 'test-routing-key')

Example "Get" based consumer
----------------------------
In this example, the python application will connect to RabbitMQ and get
messages as long as there are any in the queue, acking them after printing
information about them.

    import rmqid

    url = 'amqp://guest:guest@localhost:5672/%2F'
    connection = rmqid.Connection(url)
    channel = connection.channel()
    queue = rmqid.Queue(channel, 'example')

    # Using len on the Queue object will return the # of pending msgs in the queue
    while len(queue) > 0:
        message = queue.get()
        print 'Message:'
        print ' ID: %s' % message.properties['message_id']
        print ' Time: %s' % message.properties['timestamp'].isoformat()
        print ' Body: %s' % message.body
        message.ack()


Example Consumer
----------------
In this example, connections and channels are used as context managers along
with a queue consumer. A queue consumer is a generator that will handle
subscribing and cancelling subscriptions on a queue.

    import rmqid

    with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rmqid.Queue(channel, 'example')

            # Exit on CTRL-C
            try:

                # Consume the message
                with queue.consumer() as consumer:
                    for message in consumer.next_message():
                        print 'Message body: %s' % message.body
                        message.ack()

            except KeyboardInterrupt:
                print 'Exited consumer'
