# Message Consumer

The following example subscribes to a queue named `test` and consumes messages
until CTRL-C is pressed:

```python
#!/usr/bin/env python
import logging

import rabbitpy

URL = 'amqp://guest:guest@localhost:5672/%2f?heartbeat=15'

logging.basicConfig(level=logging.INFO)

with rabbitpy.Connection(URL) as conn:
    with conn.channel() as channel:
        try:
            for message in rabbitpy.Queue(channel, 'test'):
                message.pprint(True)
                message.ack()
        except KeyboardInterrupt:
            logging.info('Exited consumer')
```
