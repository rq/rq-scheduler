# -*- coding: utf-8 -*-
from setuptools import setup
import platform

tests_require = []
if platform.python_version() < '2.7':
    tests_require.append('discover==0.4.0')

setup(
    name='rq-scheduler',
    version='0.5.1',
    author='Selwin Ong',
    author_email='selwin.ong@gmail.com',
    packages=['rq_scheduler'],
    url='https://github.com/ui/rq-scheduler',
    license='MIT',
    description='Provides job scheduling capabilities to RQ (Redis Queue)',
    long_description=open('README.rst').read(),
    zip_safe=False,
    include_package_data=True,
    entry_points='''\
    [console_scripts]
    rqscheduler = rq_scheduler.scripts.rqscheduler:main
    ''',
    package_data = { '': ['README.rst'] },
    tests_require=tests_require,
    install_requires=['rq>=0.3.5'] + tests_require,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
