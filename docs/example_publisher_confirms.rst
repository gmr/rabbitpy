Mandatory Publishing
====================
The following example uses RabbitMQ's Publisher Confirms feature to allow for validation
that the message was successfully published::

    import rabbitpy

    # Connect to RabbitMQ on localhost, port 5672 as guest/guest
    with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:

        # Open the channel to communicate with RabbitMQ
        with conn.channel() as channel:

            # Turn on publisher confirmations
            channel.enable_publisher_confirms()

            # Create the message to publish
            message = rabbitpy.Message(channel, 'message body value')

            # Publish the message, looking for the return value to be a bool True/False
            if message.publish('test_exchange', 'test-routing-key', mandatory=True):
                print 'Message publish confirmed by RabbitMQ'
            else:
                print 'RabbitMQ indicates message publishing failure'
