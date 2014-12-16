Connection
==========
rabbitpy Connection objects are used to connect to RabbitMQ. They provide a thread-safe connection to RabbitMQ that is used to authenticate and send all channel based RPC commands over. Connections use `AMQP URI syntax <http://www.rabbitmq.com/uri-spec.html>`_ for specifying the all of the connection information, including any connection negotiation options, such as the heartbeat interval. For more information on the various query parameters that can be specified, see the `official documentation <http://www.rabbitmq.com/uri-query-parameters.html>`_.

A :class:`Connection` is a normal python object that you use:

.. code:: python

    conn = rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2F')
    conn.close()

or it can be used as a Python context manager (See :pep:`0343`):

.. code:: python

    with rabbitpy.Connection() as conn:
        # Foo

When it is used as a context manager with the `with` statement, when your code exits the block, the connection will automatically close.

If RabbitMQ remotely closes your connection via the AMQP `Connection.Close` RPC request, rabbitpy will raise the :doc:`appropriate exception <exceptions>` referenced in the request.

If heartbeats are enabled (default: 5 minutes) and RabbitMQ does not send a heartbeat request in >= 2 heartbeat intervals, a :py:class:`ConnectionResetException <rabbitpy.exceptions.ConnectionResetException>` will be raised.

API Documentation
-----------------

.. autoclass:: rabbitpy.Connection
    :members:
