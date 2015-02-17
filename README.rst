============
RQ Scheduler
============

`RQ Scheduler <https://github.com/ui/rq-scheduler>`_ is a small package that
adds job scheduling capabilities to `RQ <https://github.com/nvie/rq>`_,
a `Redis <http://redis.io/>`_ based Python queuing library.

.. image:: https://travis-ci.org/ui/rq-scheduler.svg?branch=master
    :target: https://travis-ci.org/ui/rq-scheduler

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

There are two ways you can schedule a job. The first is using RQ Scheduler's ``enqueue_at``::

    from redis import Redis
    from rq_scheduler import Scheduler
    from datetime import datetime

    scheduler = Scheduler(connection=Redis()) # Get a scheduler for the "default" queue

    # Puts a job into the scheduler. The API is similar to RQ except that it
    # takes a datetime object as first argument. So for example to schedule a
    # job to run on Jan 1st 2020 we do:
    scheduler.enqueue_at(datetime(2020, 1, 1), func)

    # Here's another example scheduling a job to run at a specific date and time (in UTC),
    # complete with args and kwargs.
    scheduler.enqueue_at(datetime(2020, 1, 1, 3, 4), func, foo, bar=baz)


The second way is using ``enqueue_in``. Instead of taking a ``datetime`` object,
this method expects a ``timedelta`` and schedules the job to run at
X seconds/minutes/hours/days/weeks later. For example, if we want to monitor how
popular a tweet is a few times during the course of the day, we could do something like::

    from datetime import timedelta

    # Schedule a job to run 10 minutes, 1 hour and 1 day later
    scheduler.enqueue_in(timedelta(minutes=10), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(hours=1), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(days=1), count_retweets, tweet_id)


------------------------
Periodic & Repeated Jobs
------------------------

As of version 0.3, `RQ Scheduler`_ also supports creating periodic and repeated jobs.
You can do this via the ``schedule`` method. Note that this feature needs
`RQ`_ >= 0.3.1.

This is how you do it::

    scheduler.schedule(
        scheduled_time=datetime.now(), # Time for first execution, in UTC timezone
        func=func,                     # Function to be queued
        args=[arg1, arg2],             # Arguments passed into function when executed
        kwargs={'foo': 'bar'},         # Keyword arguments passed into function when executed
        interval=60,                   # Time before the function is called again, in seconds
        repeat=10                      # Repeat this number of times (None means repeat forever)
    )

-------------------------
Retrieving scheduled jobs
-------------------------

Sometimes you need to know which jobs have already been scheduled. You can get a
list of enqueued jobs with the ``get_jobs`` method::

    list_of_job_instances = scheduler.get_jobs()

In it's simplest form (as seen in the above example) this method returns a list
of all job instances that are currently scheduled for execution.

Additionally the method takes two optional keyword arguments ``until`` and
``with_times``. The first one specifies up to which point in time scheduled jobs
should be returned. It can be given as either a datetime / timedelta instance
or an integer denoting the number of seconds since epoch (1970-01-01 00:00:00).
The second argument is a boolen that determines whether the scheduled execution
time should be returned along with the job instances.

Example::

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
execution using the familiar python ``in`` operator::

    if job_instance in scheduler:
        # Do something
    # or
    if job_id in scheduler:
        # Do something

---------------
Canceling a job
---------------

To cancel a job, simply do:

    scheduler.cancel(job)

---------------------
Running the scheduler
---------------------

`RQ Scheduler`_ comes with a script ``rqscheduler`` that runs a scheduler
process that polls Redis once every minute and move scheduled jobs to the
relevant queues when they need to be executed::

    # This runs a scheduler process using the default Redis connection
    rqscheduler

If you want to use a different Redis server you could also do::

    rqscheduler --host localhost --port 6379 --db 0

The script accepts these arguments:

* ``-H`` or ``--host``: Redis server to connect to
* ``-p`` or ``--port``: port to connect to
* ``-d`` or ``--db``: Redis db to use
* ``-P`` or ``--password``: password to connect to Redis

The arguments pull default values from environment variables with the
same names but with a prefix of ``RQ_REDIS_``.


Changelog
=========

Version 0.5.1
-------------
* Travis CI fixes. Thanks Steven Kryskalla!
* Modified default logging configuration. You can pass in the ``-v`` or ``--verbose`` argument
  to ``rqscheduler`` script for more verbose logging.
* RQ Scheduler now registers Queue name when a new job is scheduled. Thanks @alejandrodob !
* You can now schedule jobs with string references like ``scheduler.schedule(scheduled_time=now, func='foo.bar')``.
  Thanks @SirScott !
* ``rqscheduler`` script now accepts floating point intervals. Thanks Alexander Pikovsky!


Version 0.5.0
-------------
* IMPORTANT! Job timestamps are now stored and interpreted in UTC format.
  If you have existing scheduled jobs, you should probably change their timestamp
  to UTC before upgrading to 0.5.0. Thanks @michaelbrooks!
* You can now configure Redis connection via environment variables. Thanks @malthe!
* ``rqscheduler`` script now accepts ``--pid`` argument. Thanks @jsoncorwin!

Version 0.4.0
-------------

* Supports Python 3!
* ``Scheduler.schedule`` now allows job ``timeout`` to be specified
* ``rqscheduler`` allows Redis connection to be specified via ``--url`` argument
* ``rqscheduler`` now accepts ``--path`` argument

Version 0.3.6
-------------

* Scheduler key is not set to expire a few seconds after the next scheduling
  operation. This solves the issue of ``rqscheduler`` refusing to start after
  an unexpected shut down.

Version 0.3.5
-------------

* Support ``StrictRedis``


Version 0.3.4
-------------

* Scheduler related job attributes (``interval`` and ``repeat``) are now stored
  in ``job.meta`` introduced in RQ 0.3.4

Version 0.3.3
-------------

* You can now check whether a job is scheduled for execution using
  ``job in scheduler`` syntax
* Added ``scheduler.get_jobs`` method
* ``scheduler.enqueue`` and ``scheduler.enqueue_periodic`` will now raise a
  DeprecationWarning, please use ``scheduler.schedule`` instead

Version 0.3.2
-------------

* Periodic jobs now require `RQ`_ >= 0.3.1

Version 0.3
-----------

* Added the capability to create periodic (cron) and repeated job using ``scheduler.enqueue``
