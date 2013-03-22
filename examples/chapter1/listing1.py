import sys
sys.path.insert(0, '/Users/gmr/Source/rmqid')

import rmqid

rabbitmq_url = 'amqp://guest:guest@localhost:5672/%2f'

connection = rmqid.Connection(rabbitmq_url)
channel = connection.channel()

exchange = rmqid.Exchange(channel, 'example-exchange')
exchange.declare()

queue = rmqid.Queue(channel, 'example')
queue.declare()

queue.bind(exchange, 'example-routing-key')

for message_number in range(0, 1000):
    message = rmqid.Message(channel,
                            'Example message # #%i' % message_number,
                            {'content_type': 'text/plain'})
    message.publish(exchange, 'web.order.received')
