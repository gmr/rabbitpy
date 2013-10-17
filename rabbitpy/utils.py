"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rabbitpy to use.

"""
import collections
import types
try:
    from urllib import parse as _urlparse
except ImportError:
    import urlparse as _urlparse
from pamqp import PYTHON3

Parsed = collections.namedtuple('Parsed',
                                'scheme,netloc,url,params,query,fragment')


def parse_qs(query_string):
    return _urlparse.parse_qs(query_string)


def urlparse(url):
    value = 'http%s' % url[4:] if url[:4] == 'amqp' else url
    parsed = list(_urlparse.urlparse(value))
    parsed[0] = parsed[0].replace('http', 'amqp')
    return Parsed(*parsed)


def unquote(value):
    return _urlparse.unquote(value)


def is_string(value):
    checks = [isinstance(value, bytes), isinstance(value, str)]
    if not PYTHON3:
        checks.append(isinstance(value, unicode))
    return any(checks)
