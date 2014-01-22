Transactions
============
The :class:`Tx <rabbitpy.Tx>` or transaction class implements transactional functionality with RabbitMQ and allows for any AMQP command to be issued, then committed or rolled back.

It can be used as a normal Python object:

.. code:: python

    with rabbitpy.Connection() as connection:
        with connection.channel() as channel:
            tx = rabbitpy.Tx(channel)
            tx.select()
            exchange = rabbitpy.Exchange(channel, 'my-exchange')
            exchange.declare()
            tx.commit()

Or as a context manager (See :pep:`0343`) where the transaction will automatically be started and committed for you:

.. code:: python

    with rabbitpy.Connection() as connection:
        with connection.channel() as channel:
            with rabbitpy.Tx(channel) as tx:
                exchange = rabbitpy.Exchange(channel, 'my-exchange')
                exchange.declare()

In the event of an exception exiting the block when used as a context manager, the transaction will be rolled back for you automatically.

API Documentation
-----------------

.. autoclass:: rabbitpy.Tx
    :members:
