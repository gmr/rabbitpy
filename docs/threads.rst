Multi-threaded Use Notes
========================
To ensure that the network communication module at the core of rabbitpy is
thread safe, the :py:class:`rabbitpy.io.IO` class is a daemonic Python thread
that uses a combination of :py:class:`threading.Event`, :py:class:`Queue.Queue`,
and a local cross-platform implementation of a read-write socket pair in
:py:attr:`rabbitpy.IO.write_trigger`.

While ensuring that the core socket IO and dispatching of AMQP frames across
threads goes a long way to make sure that multi-threaded applications can safely
use rabbitpy, it does not protect against cross-thread channel utilization.

Due to the way that channels events are managed, it is recommend that you restrict
the use of a channel to an individual thread. By not sharing channels across
threads, you will ensure that you do not accidentally create issues with
channel state in the AMQP protocol. As an asynchronous RPC style protocol, when
you issue commands, such as a queue declaration, or are publishing a message,
there are expectations in the conversation on a channel about the order of
events and frames sent and received.

The following example uses the main Python thread to connect to RabbitMQ and
then spawns a thread for publishing and a thread for consuming.

.. code :: python

    import rabbitpy
    import threading

    EXCHANGE = 'threading_example'
    QUEUE = 'threading_queue'
    ROUTING_KEY = 'test'
    MESSAGE_COUNT = 100


    def consumer(connection):
        """Consume MESSAGE_COUNT messages on the connection and then exit.

        :param rabbitpy.Connection connection: The connection to consume on

        """
        received = 0
        with connection.channel() as channel:
            for message in rabbitpy.Queue(channel, QUEUE).consume_messages():
                print message.body
                message.ack()
                received += 1
                if received == MESSAGE_COUNT:
                    break


    def publisher(connection):
        """Pubilsh up to MESSAGE_COUNT messages on connection
        on an individual thread.

        :param rabbitpy.Connection connection: The connection to publish on

        """
        with connection.channel() as channel:
            for index in range(0, MESSAGE_COUNT):
                message = rabbitpy.Message(channel, 'Message #%i' % index)
                message.publish(EXCHANGE, ROUTING_KEY)


    # Connect to RabbitMQ
    with rabbitpy.Connection() as connection:

        # Open the channel, declare and bind the exchange and queue
        with connection.channel() as channel:

            # Declare the exchange
            exchange = rabbitpy.Exchange(channel, EXCHANGE)
            exchange.declare()

            # Declare the queue
            queue = rabbitpy.Queue(channel, QUEUE)
            queue.declare()

            # Bind the queue to the exchange
            queue.bind(EXCHANGE, ROUTING_KEY)


        # Pass in the kwargs
        kwargs = {'connection': connection}

        # Start the consumer thread
        consumer_thread = threading.Thread(target=consumer, kwargs=kwargs)
        consumer_thread.start()

        # Start the pubisher thread
        publisher_thread = threading.Thread(target=publisher, kwargs=kwargs)
        publisher_thread.start()

        # Join the consumer thread, waiting for it to consume all MESSAGE_COUNT messages
        consumer_thread.join()

