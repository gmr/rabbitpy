.. rabbitpy documentation master file, created by
   sphinx-quickstart on Wed Mar 27 18:31:37 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rabbitpy: RabbitMQ Simplified
=============================
rabbitpy is a pure python, thread-safe, minimalistic and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6, 2.7 and 3.3. rabbitpy aims to provide a simple and easy to use API for interfacing with RabbitMQ, minimizing the programming overhead often found in other libraries.

Installation
------------
rabbitpy is available from the `Python Package Index <https://preview-pypi.python.org/project/rabbitpy/>`_ and can be installed by running :command:`easy_install rabbitpy` or :command:`pip install rabbitpy`

rabbitpy aims to make it very simple to use RabbitMQ via the AMQP 0-9-1 protocol:

.. highlight:: python

    >>> import rabbitpy
    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
    ...                  exchange='test',
    ...                  routing_key='example',
    ...                  body={'foo': 'bar'})

Getting a message is equally simple:

.. highlight:: python

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

And consuming is nearly as simple:

.. highlight:: python

    >>> for message in rabbitpy.consume('amqp://guest:guest@localhost:5672/%2f', 'example', no_ack=True):
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

.. highlight:: python

>>> rabbitpy.create_queue('amqp://guest:guest@localhost:5672/%2f', 'test-queue')
>>> rabbitpy.create_topic_exchange('amqp://guest:guest@localhost:5672/%2f', 'my-exchange')

See `examples with other libraries <https://gist.github.com/gmr/5259929>`_ and the rest of :doc:`the simple api <simple>`, but note the simple api **is good for one-off tasks**. For more complex tasks or applications, there is a more complete, :doc:`object oriented API<api>` available for your use.

Issues
------
Please report any issues to the Github repo at `https://github.com/gmr/rabbitpy/issues <https://github.com/gmr/rabbitpy/issues>`_

Source
------
rabbitpy source is available on Github at  `https://github.com/pika/rabbitpy <https://github.com/gmr/rabbitpy>`_

.. image:: https://travis-ci.org/gmr/rabbitpy.png?branch=master   :target: https://travis-ci.org/gmr/rabbitpy

Installation
------------
rabbitpy is available as a package from the `Python Package Index <https://pypi.python.org>`_.

Examples
--------
.. toctree::
   :maxdepth: 2
   :glob:

   examples/*

API Documentation
-----------------
.. toctree::
   :glob:
   :maxdepth: 2

   simple
   api
   api/*

Inspiration
-----------
rabbitpy's simple and more pythonic interface is inspired by `Kenneth Reitz's <https://github.com/kennethreitz/>`_ awesome work on `requests <http://docs.python-requests.org/en/latest/>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

