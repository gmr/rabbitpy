import setuptools
import sys

tests_require = ['nose', 'mock']
if sys.version_info < (2, 7, 0):
    tests_require.append('unittest2')

desc = ('A pure python, thread-safe, minimalistic and pythonic RabbitMQ '
        'client library')

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'Topic :: Communications',
               'Topic :: Internet',
               'Topic :: Software Development :: Libraries']

setuptools.setup(name='rabbitpy',
                 version='0.27.1',
                 description=desc,
                 long_description=open('README.rst').read(),
                 author='Gavin M. Roy',
                 author_email='gavinmroy@gmail.com',
                 url='http://rabbitpy.readthedocs.org',
                 packages=['rabbitpy'],
                 package_data={'': ['LICENSE', 'README.md']},
                 include_package_data=True,
                 install_requires=['pamqp>=1.6.1,<2.0'],
                 tests_require=tests_require,
                 license='BSD',
                 classifiers=classifiers,
                 zip_safe=True)
