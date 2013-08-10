Declaring HA Queues
===================
The following example will create a HA :py:meth:`queue <rabbitpy.queue.Queue>` on each node in a RabbitMQ cluster.::

    import rabbitpy

    with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'example')
            queue.ha_declare()
