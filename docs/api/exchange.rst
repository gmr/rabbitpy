Exchange
========
The :class:`Exchange <rabbitpy.Exchange>` class is used to work with RabbitMQ exchanges on an open channel. The following example shows how you can create an exchange using the :class:`rabbitpy.Exchange` class.

.. code:: python

    import rabbitpy

    with rabbitpy.Connection() as connection:
        with connection.channel() as channel:
            exchange = rabbitpy.Exchange(channel, 'my-exchange')
            exchange.declare()

In addition, there are four convenience classes (:class:`DirectExchange <rabbitpy.DirectExchange>`, :class:`FanoutExchange <rabbitpy.FanoutExchange>`, :class:`HeadersExchange <rabbitpy.HeadersExchange>`, and :class:`TopicExchange <rabbitpy.TopicExchange>`) for creating each built-in exchange type in RabbitMQ.

API Documentation
-----------------

.. autoclass:: rabbitpy.Exchange
    :members:
    :inherited-members:
    :member-order: bysource

.. autoclass:: rabbitpy.DirectExchange
    :members:
    :inherited-members:
    :member-order: bysource

.. autoclass:: rabbitpy.FanoutExchange
    :members:
    :inherited-members:
    :member-order: bysource

.. autoclass:: rabbitpy.HeadersExchange
    :members:
    :inherited-members:
    :member-order: bysource

.. autoclass:: rabbitpy.TopicExchange
    :members:
    :inherited-members:
    :member-order: bysource
