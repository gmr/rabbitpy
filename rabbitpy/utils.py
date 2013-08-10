"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rmqid to use.

"""
__since__ = '2013-03-24'

from pamqp import PYTHON3

if PYTHON3:
    from urllib import parse as urlparse
else:
    import urlparse


def parse_qs(query_string):
    return urlparse.parse_qs(query_string)


def urlparse(url):
    return urlparse.urlparse(url)


def unquote(value):
    return urlparse.unquote(value)


def is_string(value):
    if PYTHON3:
        return isinstance(value, str) or isinstance(value, bytes)
    return isinstance(value, basestring)
