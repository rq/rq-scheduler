# -*- coding: utf-8 -*-
from setuptools import setup


setup(
    name='rq-scheduler',
    version='0.12.0',
    author='Selwin Ong',
    author_email='selwin.ong@gmail.com',
    packages=['rq_scheduler'],
    url='https://github.com/rq/rq-scheduler',
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
    install_requires=['crontab>=0.23.0', 'rq>=0.13', 'python-dateutil', 'freezegun'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
