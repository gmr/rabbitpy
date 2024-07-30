"""
Utilities
=========

"""
import collections
import logging
import platform
import socket
import typing
from urllib import parse

LOGGER = logging.getLogger('rabbitpy')
PYPY = platform.python_implementation() == 'PyPy'

Parsed = collections.namedtuple(
    'Parsed',
    'scheme,netloc,path,params,query,fragment,username,password,hostname,port')


def maybe_utf8_encode(value: typing.Union[str, bytes]) -> bytes:
    """Cross-python version method that will attempt to utf-8 encode a string.

    :param value: The value to maybe encode

    """
    try:
        return value.encode('utf-8')
    except AttributeError:
        return value


def urlparse(url: str) -> Parsed:
    """Parse a URL, returning a named tuple result.

    :param url: The URL to parse

    """
    value = 'http%s' % url[4:] if url[:4] == 'amqp' else url
    parsed = parse.urlparse(value)
    return Parsed(parsed.scheme.replace('http', 'amqp'), parsed.netloc,
                  parsed.path, parsed.params, parsed.query, parsed.fragment,
                  parsed.username, parsed.password, parsed.hostname,
                  parsed.port)


def trigger_write(sock: socket.socket) -> None:
    try:
        sock.send(b'0')
    except socket.error:
        pass


class DebuggingOptimizationMixin:
    """Micro-optimization to avoid logging overhead"""

    def __init__(self):
        self._debugging: typing.Optional[bool] = None

    @property
    def _is_debugging(self) -> bool:
        """Indicates that something has set the logger to ``logging.DEBUG``
        to perform a minor micro-optimization preventing ``LOGGER.debug`` calls
        when they are not required.

        :return: bool

        """
        if self._debugging is None:
            self._debugging = LOGGER.getEffectiveLevel() == logging.DEBUG
        return self._debugging
