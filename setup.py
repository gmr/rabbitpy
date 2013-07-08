from setuptools import setup
import sys

tests_require = ['mock', 'pylint', 'pep8']
if sys.version_info < (2, 7, 0):
    tests_require.append('unittest2')

desc = 'A minimalistic & pythonic AMQP library focused on supporting RabbitMQ'

setup(name='rmqid',
      version='0.4.1',
      description=desc,
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='http://github.com/gmr/rmqid',
      packages=['rmqid'],
      install_requires=['pamqp>=1.2.0', 'requests>=1.0.0'],
      tests_require=tests_require,
      test_suite='nose.collector',
      license='BSD',
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: BSD License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 2.6',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.3',
                   'Topic :: Communications',
                   'Topic :: Internet',
                   'Topic :: Software Development :: Libraries'],
      zip_safe=True)
