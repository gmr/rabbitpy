rabbitpy - rabbitmq simplified
==============================

A pure python, thread-safe, minimalistic and pythonic BSD Licensed
AMQP/RabbitMQ library that supports Python 2.6+ and Python 3.2+.
rabbitpy aims to provide a simple and easy to use API for interfacing with
RabbitMQ, minimizing the programming overhead often found in other libraries.

|PyPI version| |Downloads| |Build Status|

Installation
------------

rabbitpy may be installed via the Python package index with the tool of
your choice. I prefer pip:

::

    pip install rabbitpy

But there's always easy_install:

::

    easy_install rabbitpy

rmqid Compatibility
-------------------

rabbitpy is API compatible with rmqid.

Documentation
-------------

https://rabbitpy.readthedocs.org

Requirements
------------

-  `pamqp <https://github.com/pika/pamqp>`_

Simple Examples
---------------

The simple methods provide quick and easy access for one-off actions with
rabbitpy. In addition to the simple methods, there is extensive support for
all RabbitMQ actions using the rabbitpy object methods.

Simple Publisher
################

The simple publisher is ideal for sending one off messages:

.. code:: python

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message')

If you want to add properties:

.. code:: python

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message',
                         properties={'content_type': 'text/plain'})

And publisher confirms:

.. code:: python

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message',
                         properties={'content_type': 'text/plain'},
                         confirm=True)
    True
    >>>

Simple Getter
#############

.. code:: python

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

Simple Consumer
###############
.. code:: python

    >>> for message in rabbitpy.consume('amqp://guest:guest@localhost:5672/%2f', 'example', no_ack=True):
    ...     message.pprint(properties=True)
    ...
    Exchange: amq.topic

    Routing Key: example

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

RabbitMQ Objects
----------------
In addition, the API offers support for more complex coding using objects that
represent either the AMQ Model or RabbitMQ concepts. These include:

- Connections
- Channels
- Exchanges
- Messsages
- Policies [@TODO]
- Queues
- Transactions
- Users [@TODO]
- Virtual Hosts [ @TODO]

More complex examples and the rabbitpy API documentation are available at https://rabbitpy.readthedocs.org

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

.. |PyPI version| image:: https://badge.fury.io/py/rabbitpy.png
   :target: http://badge.fury.io/py/rabbitpy
.. |Downloads| image:: https://pypip.in/d/rabbitpy/badge.png
   :target: https://crate.io/packages/rabbitpy
.. |Build Status| image:: https://travis-ci.org/gmr/rabbitpy.png?branch=master
   :target: https://travis-ci.org/gmr/rabbitpy
