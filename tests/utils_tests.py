"""
Test the rabbitpy utils module

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import sys
from rabbitpy import utils

# 3 Unicode Compatibility hack
if sys.version_info[0] == 3:
    unicode = str


class UtilsTestCase(unittest.TestCase):

    AMQP = 'amqp://guest:guest@localhost:5672/%2F?heartbeat_interval=1'
    AMQPS = 'amqps://guest:guest@localhost:5672/%2F?heartbeat_interval=1'

    NETLOC = 'guest:guest@localhost:5672'
    PATH = '/%2F'
    PARAMS = ''
    QUERY = 'heartbeat_interval=1'
    FRAGMENT = ''

    def test_urlparse_amqp_scheme(self):
        self.assertEqual(utils.urlparse(self.AMQP).scheme, 'amqp')

    def test_urlparse_amqps_scheme(self):
        self.assertEqual(utils.urlparse(self.AMQPS).scheme, 'amqps')

    def test_urlparse_netloc(self):
        self.assertEqual(utils.urlparse(self.AMQPS).netloc, self.NETLOC)

    def test_urlparse_url(self):
        self.assertEqual(utils.urlparse(self.AMQPS).path, self.PATH)

    def test_urlparse_params(self):
        self.assertEqual(utils.urlparse(self.AMQPS).params, self.PARAMS)

    def test_urlparse_query(self):
        self.assertEqual(utils.urlparse(self.AMQPS).query, self.QUERY)

    def test_urlparse_fragment(self):
        self.assertEqual(utils.urlparse(self.AMQPS).fragment, self.FRAGMENT)

    def test_parse_qs(self):
        self.assertDictEqual(utils.parse_qs(self.QUERY),
                             {'heartbeat_interval': ['1']})

    def test_is_string_str(self):
        self.assertTrue(utils.is_string('Foo'))

    def test_is_string_bytes(self):
        self.assertTrue(utils.is_string(b'Foo'))

    @unittest.skipIf(sys.version_info[0] == 3, 'No unicode obj in 3')
    def test_is_string_unicode(self):
        self.assertTrue(utils.is_string(unicode('Foo')))

    def test_is_string_false_int(self):
        self.assertFalse(utils.is_string(123))

    def test_unqoute(self):
        self.assertEqual(utils.unquote(self.PATH), '//')
