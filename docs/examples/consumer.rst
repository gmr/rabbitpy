Message Consumer
================
The following example will subscribe to a queue named "example" and consume messages
until CTRL-C is pressed::

    import rabbitpy

    with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'example')

            # Exit on CTRL-C
            try:
                # Consume the message
                for message in queue.consume_messages():
                    print 'Message:'
                    print ' ID: %s' % message.properties['message_id']
                    print ' Time: %s' % message.properties['timestamp']
                    print ' Body: %s' % message.body
                    message.ack()

            except KeyboardInterrupt:
                print 'Exited consumer'
