# Message Getter

The following example gets messages one at a time from the `example` queue,
using `len(queue)` to check the queue depth:

```python
#!/usr/bin/env python
import rabbitpy

with rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2f') as conn:
    with conn.channel() as channel:
        queue = rabbitpy.Queue(channel, 'example')
        while len(queue) > 0:
            message = queue.get()
            message.pprint(True)
            message.ack()
            print(f'There are {len(queue)} more messages in the queue')
```
