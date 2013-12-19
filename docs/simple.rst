Simple API Methods
==================

rabbitpy's simple API methods are meant for one off use, either in your apps or in
the python interpreter. For example, if your application publishes a single
message as part of its lifetime, :py:meth:`rabbitpy.publish` should be enough
for almost any publishing concern. However if you are publishing more than
one message, it is not an efficient method to use as it connects and disconnects
from RabbitMQ on each invocation. :py:meth:`rabbitpy.get` also connects and
disconnects on each invocation. :py:meth:`rabbitpy.consume` does stay connected
as long as you're iterating through the messages returned by it. Exiting the
generator will close the connection. For a more complete api, see the rabbitpy
core API.

.. automodule:: rabbitpy.simple
    :members:
