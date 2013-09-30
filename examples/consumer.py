#!/usr/bin/env python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')

        # Exit on CTRL-C
        try:

            # Consume the message
            for message in queue.consume_messages():
                print('Message:')
                print(' ID: %s' % message.properties['message_id'])
                print(' Time: %s' % message.properties['timestamp'])
                print(' Body: %s' % message.body)
                message.ack()

        except KeyboardInterrupt:
            print('Exited consumer')
