"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rabbitpy to use.

"""
import collections
try:
    from urllib import parse as _urlparse
except ImportError:
    import urlparse as _urlparse

from pamqp import PYTHON3

Parsed = collections.namedtuple('Parsed',
                                'scheme,netloc,path,params,query,fragment,'
                                'username,password,hostname,port')


def parse_qs(query_string):
    return _urlparse.parse_qs(query_string)


def urlparse(url):
    value = 'http%s' % url[4:] if url[:4] == 'amqp' else url
    parsed = _urlparse.urlparse(value)
    return Parsed(parsed.scheme.replace('http', 'amqp'), parsed.netloc,
                  parsed.path, parsed.params, parsed.query, parsed.fragment,
                  parsed.username, parsed.password, parsed.hostname,
                  parsed.port)


def unquote(value):
    return _urlparse.unquote(value)


def is_string(value):
    checks = [isinstance(value, bytes), isinstance(value, str)]
    if not PYTHON3:
        checks.append(isinstance(value, unicode))
    return any(checks)
