# -*- coding: utf-8 -*-
import sys

from setuptools import setup

tests_require = []
if sys.version_info < (2, 7):
    tests_require.append('discover==0.4.0')

def get_dependencies():
    deps = [
        'croniter>=0.3.9',
    ]
    if (sys.version_info < (2, 7) or
            (sys.version_info >= (3, 0) and sys.version_info < (3, 1))):
        deps += ['rq~=0.8.0']
    else:
        deps += ['rq>=0.8.0']
    return deps

setup(
    name='rq-scheduler',
    version='0.6.1',
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
    package_data={'': ['README.rst']},
    tests_require=tests_require,
    install_requires=get_dependencies() + tests_require,
    classifiers=[
        'Development Status :: 4 - Beta',
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
