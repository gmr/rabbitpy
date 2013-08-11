.. rabbitpy documentation master file, created by
   sphinx-quickstart on Wed Mar 27 18:31:37 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rabbitpy: RabbitMQ Simplified
=============================
rabbitpy is a pure python, minimalistic and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6, 2.7 and 3.3. rabbitpy was previously called `rmqid`.

Installation
------------
rabbitpy is available from the `Python Package Index <https://pypi.python.org>`_ and can be installed by running :command:`easy_install rabbitpy` or :command:`pip install rabbitpy`

rabbitpy aims to make it very simple to use RabbitMQ via the AMQP 0-9-1 protocol:

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is almost as simple:

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ...     for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

See `examples with other libraries <https://gist.github.com/gmr/5259929>`_

Issues
------
Please report any issues to the Github repo at `https://github.com/gmr/rabbitpy/issues <https://github.com/gmr/rabbitpy/issues>`_

Source
------
rabbitpy source is available on Github at  `https://github.com/pika/pamqp <https://github.com/gmr/rabbitpy>`_

.. image:: https://travis-ci.org/gmr/rabbitpy.png?branch=master   :target: https://travis-ci.org/gmr/rabbitpy

Installation
------------
rabbitpy is available as a package from the `Python Package Index <https://pypi.python.org>`_.

Examples
--------
.. toctree::
   :maxdepth: 2

   example_publisher_confirms
   example_transactional_publisher
   example_consumer
   example_getter
   example_ha_queues

API Documentation
-----------------
.. toctree::
   :maxdepth: 2

   rabbitpy
   connection
   channel
   exchange
   queue
   message
   tx
   exceptions

Inspiration
-----------
rabbitpy's simple and more pythonic interface is inspired by `Kenneth Reitz's <https://github.com/kennethreitz/>`_ awesome work on `requests <http://docs.python-requests.org/en/latest/>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

