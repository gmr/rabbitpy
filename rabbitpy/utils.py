"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rabbitpy to use.

"""
from pamqp import PYTHON3

try:
    from urllib import parse as _urlparse
except ImportError:
    import urlparse as _urlparse


def parse_qs(query_string):
    return _urlparse.parse_qs(query_string)


def urlparse(url):
    return _urlparse.urlparse(url)


def unquote(value):
    return _urlparse.unquote(value)


def is_string(value):
    if PYTHON3:
        return isinstance(value, str) or isinstance(value, bytes)
    return isinstance(value, basestring)
