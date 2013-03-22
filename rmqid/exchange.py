"""
Exchange is a class that encompasses and returns the methods of the
Specification.Exchange class

"""
import logging
from pamqp import specification

from rmqid import base

LOGGER = logging.getLogger(__name__)


class Exchange(base.AMQPClass):
    """Exchange class that with methods that return the specification class
    method frames.

    """
    def __init__(self, channel, name, exchange_type='direct',
                 passive=False, durable=True, auto_delete=False):
        """Create a new instance of the queue object.

        :param rmqid.channel.Channel: The channel object to work with
        :param str name: The name of the queue
        :param str exchange_type: The exchange type
        :param bool passive: Do not create exchange
        :param bool durable: Request a durable exchange
        :param bool auto_delete: Automatically delete when not in use

        """
        super(Exchange, self).__init__(channel, name)
        self._type = exchange_type
        self._passive = passive
        self._durable = durable
        self._auto_delete = auto_delete

    def bind(self, exchange, routing_key=None):
        """Bind the exchange to another exchange with the routing key.

        :param str exchange: The exchange to bind to
        :param str routing_key: The routing key to use

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self.rpc(specification.Exchange.Bind(destination=self.name,
                                             source=exchange,
                                             routing_key=routing_key))

    def declare(self):
        """Return the Exchange.Declare frame

        :rtype: pamqp.specification.Exchange.Declare

        """
        self.rpc(specification.Exchange.Declare(exchange=self.name,
                                                exchange_type=self._type,
                                                durable=self._durable,
                                                passive=self._passive,
                                                auto_delete=self._auto_delete))

    def delete(self, if_unused=False):
        """Return the Exchange.Delete frame

        :param bool if_unused: Delete only if unused
        :rtype: pamqp.specification.Exchange.Declare

        """
        self.rpc(specification.Exchange.Delete(exchange=self.name,
                                               if_unused=if_unused))

    def unbind(self, exchange, routing_key=None):
        """Return the Exchange.Unbind the queue from the specified exchange with
        the routing key. If routing key is None, use the queue name.

        :param str exchange: The exchange to unbind from
        :param str routing_key: The routing key that binds them

        """
        if isinstance(exchange, base.AMQPClass):
            exchange = exchange.name
        self.rpc(specification.Exchange.Bind(destination=self.name,
                                             source=exchange,
                                             routing_key=routing_key))
