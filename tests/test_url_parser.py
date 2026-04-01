import unittest
import uuid

from rabbitpy import url_parser


class URLParsingTestCase(unittest.TestCase):
    def test_default_url(self):
        args = url_parser.parse()
        self.assertEqual(args['host'], 'localhost')
        self.assertEqual(args['port'], 5672)
        self.assertEqual(args['virtual_host'], '/')
        self.assertEqual(args['username'], 'guest')
        self.assertEqual(args['password'], 'guest')
        self.assertEqual(args['heartbeat'], 60)
        self.assertEqual(args['ssl'], False)
        self.assertEqual(args['timeout'], 3)

    def test_amqps(self):
        pwd = str(uuid.uuid4())
        args = url_parser.parse(f'amqps://guest:{pwd}@localhost:5671/')
        self.assertEqual(args['host'], 'localhost')
        self.assertEqual(args['port'], 5671)
        self.assertEqual(args['virtual_host'], '/')
        self.assertEqual(args['username'], 'guest')
        self.assertEqual(args['password'], pwd)
        self.assertEqual(args['heartbeat'], 60)
        self.assertEqual(args['ssl'], True)
        self.assertEqual(args['timeout'], 3)
