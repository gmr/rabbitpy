"""
Utilities
=========

"""

import collections
import logging
import platform
import socket
from urllib import parse

LOGGER = logging.getLogger('rabbitpy')
PYPY = platform.python_implementation() == 'PyPy'
PYTHON3 = True  # Always True — rabbitpy 3.x requires Python 3

Parsed = collections.namedtuple(
    'Parsed',
    'scheme,netloc,path,params,query,fragment,username,password,hostname,port',
)


def maybe_utf8_encode(value: str | bytes) -> bytes:
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
    value = f'http{url[4:]}' if url[:4] == 'amqp' else url
    parsed = parse.urlparse(value)
    return Parsed(
        parsed.scheme.replace('http', 'amqp'),
        parsed.netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment,
        parsed.username,
        parsed.password,
        parsed.hostname,
        parsed.port,
    )


def is_string(value: object) -> bool:
    """Return True if the value is a string or bytes value.

    :param value: The value to check

    """
    return isinstance(value, (str, bytes))


def parse_qs(query_string: str) -> dict[str, list[str]]:
    """Wrapper for :func:`urllib.parse.parse_qs`.

    :param query_string: The query string to parse

    """
    return parse.parse_qs(query_string)


def unquote(value: str) -> str:
    """Wrapper for :func:`urllib.parse.unquote`.

    :param value: The value to unquote

    """
    return parse.unquote(value)


def trigger_write(sock: socket.socket) -> None:
    try:
        sock.send(b'0')
    except OSError:
        pass


class DebuggingOptimizationMixin:
    """Micro-optimization to avoid logging overhead"""

    def __init__(self):
        self._debugging: bool | None = None

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
