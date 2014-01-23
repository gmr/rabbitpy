.. rabbitpy documentation master file, created by
   sphinx-quickstart on Wed Mar 27 18:31:37 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rabbitpy: RabbitMQ Simplified
=============================
rabbitpy is a pure python, thread-safe, and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6+ and 3.2+. rabbitpy aims to provide a simple and easy to use API for interfacing with RabbitMQ, minimizing the programming overhead often found in other libraries.

|Version| |Downloads|

Installation
------------
rabbitpy is available from the `Python Package Index <https://preview-pypi.python.org/project/rabbitpy/>`_ and can be installed by running :command:`easy_install rabbitpy` or :command:`pip install rabbitpy`

Usage
-----
rabbitpy aims to make it very simple to use RabbitMQ. There are simple methods to provide quick and easy access for one-off actions. In addition to the simple methods, there is extensive support for all RabbitMQ actions using the rabbitpy objects.

For simple publishing to localhost, consider the following example:

.. code:: python

    >>> rabbitpy.publish(exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

.. code:: python

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is nearly as simple:

.. code:: python

    >>> for message in rabbitpy.consume(queue_name='example', no_ack=True):
    ...     message.pprint(properties=True)
    ...
    Properties:

    {'app_id': '',
     'cluster_id': '',
     'content_encoding': '',
     'content_type': '',
     'correlation_id': '',
     'delivery_mode': None,
     'expiration': '',
     'headers': None,
     'message_id': 'b191f7f4-4e9d-4420-b18a-2ac8783ab3c5',
     'message_type': '',
     'priority': None,
     'reply_to': '',
     'timestamp': datetime.datetime(2013, 12, 18, 21, 48, 5),
     'user_id': ''}

    Body:

    'This is my test message'

Creating queues and exchanges is just as easy:

.. code:: python

>>> rabbitpy.create_queue('amqp://guest:guest@localhost:5672/%2f', 'test-queue')
>>> rabbitpy.create_topic_exchange('amqp://guest:guest@localhost:5672/%2f', 'my-exchange')

See `examples with other libraries <https://gist.github.com/gmr/5259929>`_ and the rest of :doc:`the simple api <simple>`, but note the simple api **is good for one-off tasks**. For more complex tasks or applications, there is a more complete, :doc:`object oriented API<api>` available for your use.

API Documentation
-----------------
rabbitpy is designed to have as simple and pythonic of an API as possible while
still remaining true to RabbitMQ and to the AMQP 0-9-1 specification. There are
two basic ways to interact with rabbitpy, using the simple wrapper methods:

.. toctree::
   :glob:
   :maxdepth: 2

   simple

And by using the core objects:

.. toctree::
   :glob:
   :maxdepth: 1

   api/*


Examples
--------
.. toctree::
   :maxdepth: 2
   :glob:

   examples/*

Issues
------
Please report any issues to the Github repo at `https://github.com/gmr/rabbitpy/issues <https://github.com/gmr/rabbitpy/issues>`_

Source
------
rabbitpy source is available on Github at  `https://github.com/pika/rabbitpy <https://github.com/gmr/rabbitpy>`_

|Status|

Version History
---------------
- 0.14.1: Assign queue name for RabbitMQ named queues in rabbitpy.Queue.declare
- 0.14.0: Add support for authentication_failure_close and consumer priorities, Exception cleanup, Queue consuming via Queue.__iter__, Queue & Exchange attributes are no longer private, Tx objects can be used as a context manager, and experimental support for Windows.
- 0.13.0: Validate heartbeat is always an integer, add arguments to Queue for expires, message-ttl, max-length, & dead-lettering
- 0.12.3: Minor Message.pprint() reformatting
- 0.12.2: Add Exchange and Routing Key to Message.pprint, check for empty method frames in Channel._create_message
- 0.12.1: Fix exception with pika.exceptions.AMQP
- 0.12.0: Updated simple consumer to potential one-liner, added rabbitpy.Message.pprint()
- 0.11.0: Major bugfix focused on receiving multiple AMQP frames at the same time. Add auto-coersion of property data-types.
- 0.10.0: Rewrite of IO layer yielding improved performance and reduction of CPU usage, bugfixes
- 0.9.0: Major performance improvements, CPU usage reduction, minor bug-fixes
- 0.8.0: Major bugfixes, IPv6 support
- 0.7.0: Bugfixes and code cleanup. Most notable fix around Basic.Return and recursion in Channel._wait_on_frame.
- 0.6.0: Bugfix with Queue.get(), RPC requests expecting multiple responses and the new Queue.consume_messages() method.
- 0.5.1: Installer/setup fix
- 0.5.0: Bugfix release including low level socket sending fix and connection timeouts.
- < 0.5.0: Previously called rmqid

Inspiration
-----------
rabbitpy's simple and more pythonic interface is inspired by `Kenneth Reitz's <https://github.com/kennethreitz/>`_ awesome work on `requests <http://docs.python-requests.org/en/latest/>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. |Version| image:: https://badge.fury.io/py/rabbitpy.png
   :target: http://badge.fury.io/py/rabbitpy
.. |Downloads| image:: https://pypip.in/d/rabbitpy/badge.png
   :target: https://crate.io/packages/rabbitpy
.. |Status| image:: https://travis-ci.org/gmr/rabbitpy.png?branch=master
   :target: https://travis-ci.org/gmr/rabbitpy
