.. rabbitpy documentation master file, created by
   sphinx-quickstart on Wed Mar 27 18:31:37 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rabbitpy: RabbitMQ Simplified
=============================
rabbitpy is a pure python, thread-safe [#]_, and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6+ and 3.2+. rabbitpy aims to provide a simple and easy to use API for interfacing with RabbitMQ, minimizing the programming overhead often found in other libraries.

|Version|

Installation
------------
rabbitpy is available from the `Python Package Index <https://preview-pypi.python.org/project/rabbitpy/>`_ and can be installed by running :command:`easy_install rabbitpy` or :command:`pip install rabbitpy`

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
rabbitpy source is available on Github at  `https://github.com/gmr/rabbitpy <https://github.com/gmr/rabbitpy>`_

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
