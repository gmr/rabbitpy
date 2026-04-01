import pathlib

import dotenv

_ENV_FILE = pathlib.Path(__file__).parent.parent / '.env'


class EnvironmentVariableMixin:
    @classmethod
    def setUpClass(cls):
        dotenv.load_dotenv(_ENV_FILE)
