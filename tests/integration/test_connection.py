import logging
import pathlib
import sys
import unittest

from rabbitpy import connection


class ConnectionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.os_environ = {}
        path = (
            pathlib.Path(__file__).parent.parent.parent / 'build' / 'test.env'
        )
        if not path.exists():
            sys.stderr.write('Failed to find test.env.file\n')
            return
        with path.open('r') as f:
            for line in f:
                line = line.removeprefix('export ')
                name, _, value = line.strip().partition('=')
                cls.os_environ[name] = value

    def test_connection(self):
        with connection.Connection(self.os_environ['RABBITMQ_URL']) as conn:
            self.assertIsNotNone(conn)
