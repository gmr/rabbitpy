"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rabbitpy to use.

"""
import collections
import logging
# pylint: disable=unused-import,import-error
try:
    import Queue as queue
except ImportError:
    import queue
import platform
import socket
# pylint: disable=import-error
try:
    from urllib import parse as _urlparse
except ImportError:
    import urlparse as _urlparse

from pamqp import PYTHON3

if hasattr(logging, 'NullHandler'):
    NullHandler = logging.NullHandler
else:
    class NullHandler(logging.Handler):
        """Python 2.6 does not have a NullHandler"""
        def emit(self, record):
            """Emit a record

            :param record record: The record to emit

            """
            pass


PYPY = platform.python_implementation() == 'PyPy'

Parsed = collections.namedtuple('Parsed',
                                'scheme,netloc,path,params,query,fragment,'
                                'username,password,hostname,port')


def maybe_utf8_encode(value):
    """Cross-python version method that will attempt to utf-8 encode a string.

    :param mixed value: The value to maybe encode
    :return: str

    """

    if PYTHON3:
        if is_string(value) and not isinstance(value, bytes):
            return bytes(value, 'utf-8')
        return value
    if isinstance(value, unicode):  # pylint: disable=undefined-variable
        return value.encode('utf-8')
    return value


def parse_qs(query_string):
    """Cross-python version method for parsing a query string.

    :param str query_string: The query string to parse
    :return: tuple
    """
    return _urlparse.parse_qs(query_string)


def urlparse(url):
    """Parse a URL, returning a named tuple result.

    :param str url: The URL to parse
    :rtype: collections.namedtuple

    """
    value = 'http%s' % url[4:] if url[:4] == 'amqp' else url
    parsed = _urlparse.urlparse(value)
    return Parsed(parsed.scheme.replace('http', 'amqp'), parsed.netloc,
                  parsed.path, parsed.params, parsed.query, parsed.fragment,
                  _urlparse.unquote(parsed.username),
                  _urlparse.unquote(parsed.password),
                  parsed.hostname, parsed.port)


def unquote(value):
    """Cross-python version method for unquoting a URI value.

    :param str value: The value to unquote
    :rtype: str

    """
    return _urlparse.unquote(value)


def is_string(value):
    """Check to see if the value is a string in Python 2 and 3.

    :param bytes|str|unicode value: The value to check
    :rtype: bool

    """
    checks = [isinstance(value, bytes), isinstance(value, str)]
    if not PYTHON3:
        # pylint: disable=undefined-variable
        checks.append(isinstance(value, unicode))
    return any(checks)


def trigger_write(sock):
    """Notifies the IO loop we need to write a frame by writing a byte
    to a local socket.

    """
    try:
        sock.send(b'0')
    except socket.error:
        pass
