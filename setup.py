import setuptools

desc = ('A pure python, thread-safe, minimalistic and pythonic RabbitMQ '
        'client library')

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: 3.6',
               'Programming Language :: Python :: 3.7',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'Topic :: Communications',
               'Topic :: Internet',
               'Topic :: Software Development :: Libraries']

setuptools.setup(name='rabbitpy',
                 version='2.0.1',
                 description=desc,
                 long_description=open('README.rst').read(),
                 author='Gavin M. Roy',
                 author_email='gavinmroy@gmail.com',
                 url='https://rabbitpy.readthedocs.io',
                 packages=['rabbitpy'],
                 package_data={'': ['LICENSE', 'README.md']},
                 include_package_data=True,
                 install_requires=['pamqp>=2,<3'],
                 license='BSD',
                 classifiers=classifiers,
                 zip_safe=True)
