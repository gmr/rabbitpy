#!/usr/bin/env python2
import rmqid

with rmqid.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rmqid.Queue(channel, 'example')

        # Exit on CTRL-C
        try:

            # Consume the message
            with queue.consumer() as consumer:
                for message in consumer.next_message():
                    print 'Message:'
                    print ' ID: %s' % message.properties['message_id']
                    print ' Time: %s' % message.properties['timestamp']
                    print ' Body: %s' % message.body
                    message.ack()

        except KeyboardInterrupt:
            print 'Exited consumer'