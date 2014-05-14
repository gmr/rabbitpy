rabbitpy - rabbitmq simplified
==============================

A pure python, thread-safe, minimalistic and pythonic BSD Licensed
AMQP/RabbitMQ library that supports Python 2.6+ and Python 3.2+.
rabbitpy aims to provide a simple and easy to use API for interfacing with
RabbitMQ, minimizing the programming overhead often found in other libraries.

|Version| |Downloads| |Status| |Coverage| |License|

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
- Messages
- Policies [@TODO]
- Queues
- Transactions
- Users [@TODO]
- Virtual Hosts [ @TODO]

More complex examples and the rabbitpy API documentation are available at https://rabbitpy.readthedocs.org

Version History
---------------
Available at https://rabbitpy.readthedocs.org/en/latest/history.html

.. |Version| image:: https://badge.fury.io/py/rabbitpy.svg?
   :target: http://badge.fury.io/py/rabbitpy

.. |Status| image:: https://travis-ci.org/gmr/rabbitpy.svg?branch=master
   :target: https://travis-ci.org/gmr/rabbitpy

.. |Coverage| image:: https://coveralls.io/repos/gmr/rabbitpy/badge.png
   :target: https://coveralls.io/r/gmr/rabbitpy
  
.. |Downloads| image:: https://pypip.in/d/rabbitpy/badge.svg?
   :target: https://pypi.python.org/pypi/rabbitpy
   
.. |License| image:: https://pypip.in/license/rabbitpy/badge.svg?
   :target: https://rabbitpy.readthedocs.org
