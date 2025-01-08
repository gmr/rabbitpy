import pathlib
import sys


class EnvironmentVariableMixin:

    @classmethod
    def setUpClass(cls):
        cls.os_environ = {}
        path = pathlib.Path(__file__).parent.parent \
               / 'build' / 'test.env'
        if not path.exists():
            sys.stderr.write('Failed to find test.env.file\n')
            return
        with path.open('r') as f:
            for line in f:
                if line.startswith('export '):
                    line = line[7:]
                name, _, value = line.strip().partition('=')
                cls.os_environ[name] = value
