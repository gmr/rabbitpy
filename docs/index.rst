.. rabbitpy documentation master file, created by
   sphinx-quickstart on Wed Mar 27 18:31:37 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rabbitpy: RabbitMQ Simplified
=============================
rabbitpy is a pure python, thread-safe [#]_, and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6+ and 3.2+. rabbitpy aims to provide a simple and easy to use API for interfacing with RabbitMQ, minimizing the programming overhead often found in other libraries.

|Version| |Downloads| |License|

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

See `examples with other libraries <https://gist.github.com/gmr/5259929>`_ and the rest of :doc:`the simple api <simple>`, but note the simple api **is good for one-off tasks**. For more complex tasks or applications, there is a more complete, object oriented api available for your use.

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

.. [#] If you're looking to use rabbitpy in a multi-threaded application, you should the notes about multi-threaded use in :doc:`threads`.

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
See :doc:`history`

Inspiration
-----------
rabbitpy's simple and more pythonic interface is inspired by `Kenneth Reitz's <https://github.com/kennethreitz/>`_ awesome work on `requests <http://docs.python-requests.org/en/latest/>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. |Version| image:: https://badge.fury.io/py/rabbitpy.svg?
   :target: http://badge.fury.io/py/rabbitpy

.. |Status| image:: https://travis-ci.org/gmr/rabbitpy.svg?branch=master
   :target: https://travis-ci.org/gmr/rabbitpy

.. |Downloads| image:: https://pypip.in/d/rabbitpy/badge.svg?
   :target: https://pypi.python.org/pypi/rabbitpy

.. |License| image:: https://pypip.in/license/rabbitpy/badge.svg?
   :target: https://rabbitpy.readthedocs.org