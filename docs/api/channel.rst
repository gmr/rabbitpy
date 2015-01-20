Channel
=======
A :class:`Channel` is created on an active connection using the :meth:`Connection.channel() <rabbitpy.Connection.channel>` method. Channels can act as normal Python objects:

.. code:: python

    conn = rabbitpy.Connection()
    chan = conn.channel()
    chan.enable_publisher_confirms()
    chan.close()

or as a Python context manager (See :pep:`0343`):

.. code:: python

    with rabbitpy.Connection() as conn:
        with conn.channel() as chan:
            chan.enable_publisher_confirms()

When they are used as a context manager with the `with` statement, when your code exits the block, the channel will automatically close, issuing a clean shutdown with RabbitMQ via the ``Channel.Close`` RPC request.

You should be aware that if you perform actions on a channel with exchanges, queues, messages or transactions that RabbitMQ does not like, it will close the channel by sending an AMQP ``Channel.Close`` RPC request to your application. Upon receipt of such a request, rabbitpy will raise the :doc:`appropriate exception <exceptions>` referenced in the request.

API Documentation
-----------------

.. autoclass:: rabbitpy.Channel
    :members:
