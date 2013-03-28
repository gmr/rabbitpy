Message Getter
==============
The following example will get a single message at a time from the "example" queue
as long as there are messages in it. It uses :code:`len(queue)` to check the current
queue depth while it is looping::

    import rmqid

    with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rmqid.Queue(channel, 'example')

            while len(queue) > 0:
                message = queue.get()
                print 'Message:'
                print ' ID: %s' % message.properties['message_id']
                print ' Time: %s' % message.properties['timestamp'].isoformat()
                print ' Body: %s' % message.body
                message.ack()

                print 'There are %i more messages in the queue' % len(queue)
