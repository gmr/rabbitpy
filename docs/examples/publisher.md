# Publisher

The following example connects to RabbitMQ and publishes a message to an exchange:

```python
#!/usr/bin/env python
import datetime
import uuid

import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        exchange = rabbitpy.Exchange(channel, 'example_exchange')
        exchange.declare()

        message = rabbitpy.Message(
            channel,
            'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
            {
                'content_type': 'text/plain',
                'delivery_mode': 1,
                'message_type': 'Lorem ipsum',
                'timestamp': datetime.datetime.now(tz=datetime.UTC),
                'message_id': str(uuid.uuid4()),
            },
        )
        message.publish(exchange, 'test-routing-key')
```
