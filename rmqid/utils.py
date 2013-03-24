"""Utilities to make Python 3 support easier, providing wrapper methods which
can call the appropriate method for either Python 2 or Python 3 but creating
a single API point for rmqid to use.

"""
__since__ = '2013-03-24'

from pamqp import PYTHON3

if PYTHON3:
    from urllib import parse
else:
    import urlparse as parse


def urlparse(url):
    return parse.urlparse(url)


def unquote(value):
    return parse.unquote(value)


def is_string(value):
    if PYTHON3:
        return isinstance(value, str) or isinstance(value, bytes)
    return isinstance(value, basestring)
