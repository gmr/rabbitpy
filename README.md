rabbitpy - rabbitmq simplified
===========================
A pure python, minimalistic and pythonic BSD Licensed AMQP/RabbitMQ library that supports Python 2.6, 2.7 and 3.3.

Version
-------
The current released alpha version is 0.4.3

Installation
------------
rabbitpy may be installed via the Python package index with the tool of your choice. I prefer pip:

    pip install rabbitpy

But there's always easy_install:

    easy_install rabbitpy

Documentation
-------------

https://rabbitpy.readthedocs.org

Requirements
------------
 - pamqp - https://github.com/pika/pamqp

Python3 Caveats
---------------
 - Message bodies must use the bytes data type while most other values are strings.

Examples
========

Simple Publisher
----------------
The simple publisher is ideal for sending one off messages:

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message')

If you want to add properties:

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message',
                         properties={'content_type': 'text/plain'})

And publisher confirms:

    >>> rabbitpy.publish('amqp://guest:guest@localhost:5672/%2f',
                         exchange='test',
                         routing_key='example',
                         body='This is my test message',
                         properties={'content_type': 'text/plain'},
                         confirm=True)
    True
    >>>

Simple Getter
-------------

    >>> m = rabbitpy.get('amqp://guest:guest@localhost:5672/%2f', 'test')
    >>> m.json()
    {u'foo': u'bar'}

Simple Consumer
---------------

    >>> with rabbitpy.consumer('amqp://guest:guest@localhost:5672/%2f', 'test') as c:
    ..    for message in c.next_message():
    ...         print message.properties['message_id']
    ...         print message.body
    ...         message.ack()
    ...
    856dfdc7-5ee3-4fc1-9635-977bf0043a9f
    {"foo": "bar"}

More complex examples are available at https://rabbitpy.readthedocs.org
