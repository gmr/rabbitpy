Exceptions
==========
rabbitpy contains two types of exceptions, exceptions that are specific to rabbitpy and exceptions that are raises as the result of a Channel or Connection closure from RabbitMQ. These exceptions will be raised to let you know when you have performed an action like redeclared a pre-existing queue with different values. Consider the following example:

.. code:: python

    >>> import rabbitpy
    >>>
    >>> with rabbitpy.Connection() as connection:
    ...     with connection.channel() as channel:
    ...         queue = rabbitpy.Queue(channel, 'exception-test')
    ...         queue.durable = True
    ...         queue.declare()
    ...         queue.durable = False
    ...         queue.declare()
    ...
    Traceback (most recent call last):
      File "<stdin>", line 7, in <module>
      File "rabbitpy/connection.py", line 131, in __exit__
        self._shutdown_connection()
      File "rabbitpy/connection.py", line 469, in _shutdown_connection
        self._channels[chan_id].close()
      File "rabbitpy/channel.py", line 124, in close
        super(Channel, self).close()
      File "rabbitpy/base.py", line 185, in close
        self.rpc(frame_value)
      File "rabbitpy/base.py", line 199, in rpc
        self._write_frame(frame_value)
      File "rabbitpy/base.py", line 311, in _write_frame
        raise exception
    rabbitpy.exceptions.AMQPPreconditionFailed: <pamqp.specification.Channel.Close object at 0x10e86bd50>

In this example, the channel that was created on the second line was closed and RabbitMQ is raising the :class:`AMQPPreconditionFailed <rabbitpy.exceptions.AMQPPreconditionFailed>` exception via RPC sent to your application using the AMQP Channel.Close method.

.. automodule:: rabbitpy.exceptions
   :members:
   :private-members:
   :undoc-members:

