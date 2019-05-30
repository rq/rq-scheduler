============
RQ Scheduler
============

`RQ Scheduler <https://github.com/rq/rq-scheduler>`_ is a small package that
adds job scheduling capabilities to `RQ <https://github.com/nvie/rq>`_,
a `Redis <http://redis.io/>`_ based Python queuing library.

.. image:: https://travis-ci.org/rq/rq-scheduler.svg?branch=master
    :target: https://travis-ci.org/rq/rq-scheduler

====================
Support RQ Scheduler
====================

If you find ``rq-scheduler`` useful, please consider supporting its development via `Tidelift <https://tidelift.com/subscription/pkg/pypi-rq_scheduler?utm_source=pypi-rq-scheduler&utm_medium=referral&utm_campaign=readme>`_.

============
Requirements
============

* `RQ`_

============
Installation
============

You can install `RQ Scheduler`_ via pip::

    pip install rq-scheduler

Or you can download the latest stable package from `PyPI <http://pypi.python.org/pypi/rq-scheduler>`_.

=====
Usage
=====

Schedule a job involves doing two different things:

1. Putting a job in the scheduler
2. Running a scheduler that will move scheduled jobs into queues when the time comes

----------------
Scheduling a Job
----------------

There are two ways you can schedule a job. The first is using RQ Scheduler's ``enqueue_at``

.. code-block:: python

    from redis import Redis
    from rq import Queue
    from rq_scheduler import Scheduler
    from datetime import datetime

    scheduler = Scheduler(connection=Redis()) # Get a scheduler for the "default" queue
    scheduler = Scheduler('foo', connection=Redis()) # Get a scheduler for the "foo" queue

    # You can also instantiate a Scheduler using an RQ Queue
    queue = Queue('bar', connection=Redis())
    scheduler = Scheduler(queue=queue)

    # Puts a job into the scheduler. The API is similar to RQ except that it
    # takes a datetime object as first argument. So for example to schedule a
    # job to run on Jan 1st 2020 we do:
    scheduler.enqueue_at(datetime(2020, 1, 1), func) # Date time should be in UTC

    # Here's another example scheduling a job to run at a specific date and time (in UTC),
    # complete with args and kwargs.
    scheduler.enqueue_at(datetime(2020, 1, 1, 3, 4), func, foo, bar=baz)


The second way is using ``enqueue_in``. Instead of taking a ``datetime`` object,
this method expects a ``timedelta`` and schedules the job to run at
X seconds/minutes/hours/days/weeks later. For example, if we want to monitor how
popular a tweet is a few times during the course of the day, we could do something like

.. code-block:: python

    from datetime import timedelta

    # Schedule a job to run 10 minutes, 1 hour and 1 day later
    scheduler.enqueue_in(timedelta(minutes=10), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(hours=1), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(days=1), count_retweets, tweet_id)

**IMPORTANT**: You should always use UTC datetime when working with `RQ Scheduler`_.

------------------------
Periodic & Repeated Jobs
------------------------

As of version 0.3, `RQ Scheduler`_ also supports creating periodic and repeated jobs.
You can do this via the ``schedule`` method. Note that this feature needs
`RQ`_ >= 0.3.1.

This is how you do it

.. code-block:: python

    scheduler.schedule(
        scheduled_time=datetime.utcnow(), # Time for first execution, in UTC timezone
        func=func,                     # Function to be queued
        args=[arg1, arg2],             # Arguments passed into function when executed
        kwargs={'foo': 'bar'},         # Keyword arguments passed into function when executed
        interval=60,                   # Time before the function is called again, in seconds
        repeat=10,                     # Repeat this number of times (None means repeat forever)
        meta={'foo': 'bar'}            # Arbitrary pickleable data on the job itself
    )

**IMPORTANT NOTE**: If you set up a repeated job, you must make sure that you
either do not set a `result_ttl` value or you set a value larger than the interval.
Otherwise, the entry with the job details will expire and the job will not get re-scheduled.

------------------------
Cron Jobs
------------------------

As of version 0.6.0, `RQ Scheduler`_ also supports creating Cron Jobs, which you can use for
repeated jobs to run periodically at fixed times, dates or intervals, for more info check
https://en.wikipedia.org/wiki/Cron. You can do this via the ``cron`` method.

This is how you do it

.. code-block:: python

    scheduler.cron(
        cron_string,                # A cron string (e.g. "0 0 * * 0")
        func=func,                  # Function to be queued
        args=[arg1, arg2],          # Arguments passed into function when executed
        kwargs={'foo': 'bar'},      # Keyword arguments passed into function when executed
        repeat=10,                  # Repeat this number of times (None means repeat forever)
        queue_name=queue_name,      # In which queue the job should be put in
        meta={'foo': 'bar'}         # Arbitrary pickleable data on the job itself
    )

-------------------------
Retrieving scheduled jobs
-------------------------

Sometimes you need to know which jobs have already been scheduled. You can get a
list of enqueued jobs with the ``get_jobs`` method

.. code-block:: python

    list_of_job_instances = scheduler.get_jobs()

In it's simplest form (as seen in the above example) this method returns a list
of all job instances that are currently scheduled for execution.

Additionally the method takes two optional keyword arguments ``until`` and
``with_times``. The first one specifies up to which point in time scheduled jobs
should be returned. It can be given as either a datetime / timedelta instance
or an integer denoting the number of seconds since epoch (1970-01-01 00:00:00).
The second argument is a boolean that determines whether the scheduled execution
time should be returned along with the job instances.

Example

.. code-block:: python

    # get all jobs until 2012-11-30 10:00:00
    list_of_job_instances = scheduler.get_jobs(until=datetime(2012, 10, 30, 10))

    # get all jobs for the next hour
    list_of_job_instances = scheduler.get_jobs(until=timedelta(hours=1))

    # get all jobs with execution times
    jobs_and_times = scheduler.get_jobs(with_times=True)
    # returns a list of tuples:
    # [(<rq.job.Job object at 0x123456789>, datetime.datetime(2012, 11, 25, 12, 30)), ...]

------------------------------
Checking if a job is scheduled
------------------------------

You can check whether a specific job instance or job id is scheduled for
execution using the familiar python ``in`` operator

.. code-block:: python

    if job_instance in scheduler:
        # Do something
    # or
    if job_id in scheduler:
        # Do something

---------------
Canceling a job
---------------

To cancel a job, simply pass a ``Job`` or a job id to ``scheduler.cancel``

.. code-block:: python

    scheduler.cancel(job)

Note that this method returns ``None`` whether the specified job was found or not.

---------------------
Running the scheduler
---------------------

`RQ Scheduler`_ comes with a script ``rqscheduler`` that runs a scheduler
process that polls Redis once every minute and move scheduled jobs to the
relevant queues when they need to be executed

.. code-block:: bash

    # This runs a scheduler process using the default Redis connection
    rqscheduler

If you want to use a different Redis server you could also do

.. code-block:: bash

    rqscheduler --host localhost --port 6379 --db 0

The script accepts these arguments:

* ``-H`` or ``--host``: Redis server to connect to
* ``-p`` or ``--port``: port to connect to
* ``-d`` or ``--db``: Redis db to use
* ``-P`` or ``--password``: password to connect to Redis
* ``-b`` or ``--burst``: runs in burst mode (enqueue scheduled jobs whose execution time is in the past and quit)
* ``-i INTERVAL`` or ``--interval INTERVAL``: How often the scheduler checks for new jobs to add to the queue (in seconds, can be floating-point for more precision).
* ``-j`` or ``--job-class``: specify custom job class for rq to use (python module.Class)
* ``-q`` or ``--queue-class``: specify custom queue class for rq to use (python module.Class)

The arguments pull default values from environment variables with the
same names but with a prefix of ``RQ_REDIS_``.

Running the Scheduler as a Service on Ubuntu
--------------------------------------------

sudo /etc/systemd/system/rqscheduler.service

.. code-block:: bash
    
    [Unit]
    Description=RQScheduler
    After=network.target

    [Service]
    ExecStart=/home/<<User>>/.virtualenvs/<<YourVirtualEnv>>/bin/python \
        /home/<<User>>/.virtualenvs/<<YourVirtualEnv>>/lib/<<YourPythonVersion>>/site-packages/rq_scheduler/scripts/rqscheduler.py

    [Install]
    WantedBy=multi-user.target

You will also want to add any command line parameters if your configuration is not localhost or not set in the environmnt variabes.  

Start, check Status and Enable the service

.. code-block:: bash

    sudo systemctl start rqscheduler.service
    sudo systemctl status rqscheduler.service
    sudo systemctl enable rqscheduler.service
