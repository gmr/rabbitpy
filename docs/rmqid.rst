rmqid Core API
==============
rmqid is designed to have as simple and pythonic of an API as possible while
stil remaining true to the AMQP 0-9-1 specification. There are two basic ways
to interact with rmqid, using the simple wrapper methods and using the core
objects.

Simple Methods
--------------
rmqid's simple methods are meant for one off use, either in your apps or in
the python interpreter. For example, if your application publishes a single
message as part of its lifetime, :py:meth:`rmqid.publish` should be enough
for almost any publishing concern. However if you are publishing more than
one message, it is not an efficient method to use as it connects and disconnects
from RabbitMQ on each invocation. :py:meth:`rmqid.get` also connects and
disconnects on each invocation. :py:meth:`rmqid.consume` does stay connected
as long as you're using the :py:meth:`rmqid.queue.Consumer.next_method`
generator.

.. automodule:: rmqid.simple
    :members:

Core Objects
------------

.. autoclass:: rmqid.connection.Connection
    :members:
    :noindex:

.. autoclass:: rmqid.channel.Channel
    :members:
    :noindex:

.. autoclass:: rmqid.exchange.Exchange
    :members:
    :noindex:


.. autoclass:: rmqid.queue.Queue
    :members:
    :noindex:

.. autoclass:: rmqid.message.Message
    :members:
    :noindex:

.. autoclass:: rmqid.tx.Tx
    :members:
    :noindex:
