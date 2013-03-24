from setuptools import setup

setup(name='rmqid',
      version='0.1.0',
      description='Minimalistic and Python RabbitMQ Focused AMQP library',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='http://github.com/gmr/rmqid',
      packages=['rmqid'],
      install_requires=['pamqp'],
      tests_require=['mock', 'unittest2', 'pylint', 'pep8'],
      test_suite = "nose.collector",
      license='BSD',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Operating System :: OS Independent',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          ],
      zip_safe=True)
