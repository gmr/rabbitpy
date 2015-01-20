AMQP Adapter
============
While the core rabbitpy API strives to provide an easy to use, Pythonic interface
for RabbitMQ, some developers may prefer a less opinionated AMQP interface. The
:py:class:`rabbitpy.AMQP` adapter provides a more traditional AMQP client library
API seen in libraries like `pika <http://pika.readthedocs.org>`_.

.. versionadded:: 0.26

Example
-------
The following example will connect to RabbitMQ and use the :py:class:`rabbitpy.AMQP`
adapter to consume and acknowledge messages.

.. code:: python

    import rabbitpy

    with rabbitpy.Connection() as conn:
        with conn.channel() as channel:
            amqp = rabbitpy.AMQP(channel)

            for message in amqp.basic_consume('queue-name'):
                print(message)

API Documentation
-----------------

.. autoclass:: rabbitpy.AMQP
    :members:
