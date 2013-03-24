from setuptools import setup
import sys

tests_require = ['mock', 'pylint', 'pep8']
if sys.version_info < (2, 7, 0):
    tests_require.append('unittest2')

desc = 'A minimalistic & pythonic AMQP library focused on supporting RabbitMQ'

setup(name='rmqid',
      version='0.1.0',
      description=desc,
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='http://github.com/gmr/rmqid',
      packages=['rmqid'],
      install_requires=['pamqp'],
      tests_require=tests_require,
      test_suite='nose.collector',
      license='BSD',
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: BSD License',
                   'Operating System :: OS Independent',
                   'Topic :: Communications',
                   'Topic :: Internet',
                   'Topic :: Software Development :: Libraries'],
      zip_safe=True)
