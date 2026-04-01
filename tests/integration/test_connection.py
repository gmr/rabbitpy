import logging
import os
import pathlib
import unittest

import dotenv

from rabbitpy import connection

dotenv.load_dotenv(pathlib.Path(__file__).parent.parent.parent / '.env')


class ConnectionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

    @unittest.skipUnless(
        os.environ.get('RABBITMQ_URL'), 'RABBITMQ_URL not set'
    )
    def test_connection(self):
        with connection.Connection(os.environ['RABBITMQ_URL']) as conn:
            self.assertIsNotNone(conn)
