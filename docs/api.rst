rabbitpy Core API
=================

rabbitpy is designed to have as simple and pythonic of an API as possible while
stil remaining true to the AMQP 0-9-1 specification. There are two basic ways
to interact with rabbitpy, using the simple wrapper methods and using the core
objects.

.. autoclass:: rabbitpy.connection.Connection
    :members:
    :noindex:

.. autoclass:: rabbitpy.channel.Channel
    :members:
    :noindex:

.. autoclass:: rabbitpy.exchange.Exchange
    :members:
    :noindex:


.. autoclass:: rabbitpy.amqp_queue.Queue
    :members:
    :noindex:

.. autoclass:: rabbitpy.message.Message
    :members:
    :noindex:

.. autoclass:: rabbitpy.tx.Tx
    :members:
    :noindex:
