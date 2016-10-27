Message Getter
==============
The following example will get a single message at a time from the "example" queue
as long as there are messages in it. It uses :code:`len(queue)` to check the current
queue depth while it is looping::

    import rabbitpy

    with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'example')
            while len(queue) > 0:
                message = queue.get()
                message.pprint(True)
                message.ack()
                print('There are {} more messages in the queue'.format(len(queue)))
