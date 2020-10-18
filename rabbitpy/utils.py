"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rabbitpy to use.

"""
import collections
import queue
import platform
import socket
from urllib import parse as _urlparse

PYPY = platform.python_implementation() == 'PyPy'

Parsed = collections.namedtuple('Parsed',
                                'scheme,netloc,path,params,query,fragment,'
                                'username,password,hostname,port')


def maybe_utf8_encode(value):
    """Cross-python version method that will attempt to utf-8 encode a string.

    :param mixed value: The value to maybe encode
    :return: str

    """

    if is_string(value) and not isinstance(value, bytes):
        return bytes(value, 'utf-8')
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
                  parsed.username,
                  parsed.password,
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
    return any(checks)


def trigger_write(sock):
    """Notifies the IO loop we need to write a frame by writing a byte
    to a local socket.

    """
    try:
        sock.send(b'0')
    except socket.error:
        pass
