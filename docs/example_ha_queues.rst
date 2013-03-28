Declaring HA Queues
===================
The following example will create a HA :py:meth:`queue <rmqid.queue.Queue>` on each node in a RabbitMQ cluster.::

    import rmqid

    with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rmqid.Queue(channel, 'example')
            queue.ha_declare()
