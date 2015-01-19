Queue
=====
The :class:`Queue <rabbitpy.Queue>` class is used to work with RabbitMQ queues on an open channel. The following example shows how you can create a queue using the :meth:`Queue.declare <rabbitpy.Queue.declare>` method.

.. code:: python

    import rabbitpy

    with rabbitpy.Connection() as connection:
        with connection.channel() as channel:
            queue = rabbitpy.Queue(channel, 'my-queue')
            queue.durable = True
            queue.declare()

To consume messages you can iterate over the Queue object itself if the defaults for the :py:meth:`Queue.__iter__() <Queue.__iter__>` method work for your needs:

.. code:: python

    with conn.channel() as channel:
        for message in rabbitpy.Queue(channel, 'example'):
            print 'Message: %r' % message
            message.ack()

or by the :py:meth:`Queue.consume() <rabbitpy.Queue.consume>` method if you would like to specify `no_ack`, `prefetch_count`, or `priority`:

.. code:: python

    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')
        for message in queue.consume():
            print 'Message: %r' % message
            message.ack()

.. warning:: If you use either the :py:class:`Queue` as an iterator method or :py:meth:`Queue.consume` method of consuming messages in PyPy,
             you must manually invoke :py:meth:`Queue.stop_consuming`. This is due to PyPy not predictably cleaning up after the generator
             used for allowing the iteration over messages. Should your code want to test to see if the code is being executed in PyPy,
             you can evaluate the boolean ``rabbitpy.PYPY`` constant value.

API Documentation
-----------------

.. autoclass:: rabbitpy.Queue
    :members:
    :special-members:
