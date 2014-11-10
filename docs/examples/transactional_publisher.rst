Transactional Publisher
========================
The following example uses RabbitMQ's Transactions feature to send the message,
then roll it back::

    import rabbitpy

    # Connect to RabbitMQ on localhost, port 5672 as guest/guest
    with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:

        # Open the channel to communicate with RabbitMQ
        with conn.channel() as channel:

            # Start the transaction
            tx = rabbitpy.Tx(channel)
            tx.select()

            # Create the message to publish & publish it
            message = rabbitpy.Message(channel, 'message body value')
            message.publish('test_exchange', 'test-routing-key')

            # Rollback the transaction
            tx.rollback()
