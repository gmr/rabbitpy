rabbitpy Core API
=================
rabbitpy is designed to have as simple and pythonic of an API as possible while
stil remaining true to the AMQP 0-9-1 specification. There are two basic ways
to interact with rabbitpy, using the simple wrapper methods and using the core
objects.

Simple Methods
--------------
rabbitpy's simple methods are meant for one off use, either in your apps or in
the python interpreter. For example, if your application publishes a single
message as part of its lifetime, :py:meth:`rabbitpy.publish` should be enough
for almost any publishing concern. However if you are publishing more than
one message, it is not an efficient method to use as it connects and disconnects
from RabbitMQ on each invocation. :py:meth:`rabbitpy.get` also connects and
disconnects on each invocation. :py:meth:`rabbitpy.consume` does stay connected
as long as you're using the :py:meth:`rabbitpy.queue.Consumer.next_method`
generator.

.. automodule:: rabbitpy.simple
    :members:

Core Objects
------------

.. autoclass:: rabbitpy.connection.Connection
    :members:
    :noindex:

.. autoclass:: rabbitpy.channel.Channel
    :members:
    :noindex:

.. autoclass:: rabbitpy.exchange.Exchange
    :members:
    :noindex:


.. autoclass:: rabbitpy.queue.Queue
    :members:
    :noindex:

.. autoclass:: rabbitpy.message.Message
    :members:
    :noindex:

.. autoclass:: rabbitpy.tx.Tx
    :members:
    :noindex:
