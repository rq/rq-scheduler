import logging
import signal
import time

from datetime import datetime
from itertools import repeat

from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.queue import Queue
from rq.utils import backend_class

from redis import WatchError

from .utils import from_unix, to_unix, get_next_scheduled_time, rationalize_until

logger = logging.getLogger(__name__)


class Scheduler(object):
    scheduler_key = 'rq:scheduler'
    scheduled_jobs_key = 'rq:scheduler:scheduled_jobs'
    queue_class = Queue
    job_class = Job

    def __init__(self, queue_name='default', queue=None, interval=60, connection=None,
                 job_class=None, queue_class=None):
        from rq.connections import resolve_connection
        self.connection = resolve_connection(connection)
        self._queue = queue
        if self._queue is None:
            self.queue_name = queue_name
        else:
            self.queue_name = self._queue.name
        self._interval = interval
        self.log = logger
        self._lock_acquired = False
        self.job_class = backend_class(self, 'job_class', override=job_class)
        self.queue_class = backend_class(self, 'queue_class',
                                         override=queue_class)

    def register_birth(self):
        self.log.info('Registering birth')
        if self.connection.exists(self.scheduler_key) and \
                not self.connection.hexists(self.scheduler_key, 'death'):
            raise ValueError("There's already an active RQ scheduler")

        key = self.scheduler_key
        now = time.time()

        with self.connection.pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            # Set scheduler key to expire a few seconds after polling interval
            # This way, the key will automatically expire if scheduler
            # quits unexpectedly
            p.expire(key, int(self._interval) + 10)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.info('Registering death')
        with self.connection.pipeline() as p:
            p.hset(self.scheduler_key, 'death', time.time())
            p.expire(self.scheduler_key, 60)
            p.execute()

    def acquire_lock(self):
        """
        Acquire lock before scheduling jobs to prevent another scheduler
        from scheduling jobs at the same time.

        This function returns True if a lock is acquired. False otherwise.
        """
        key = '%s_lock' % self.scheduler_key
        now = time.time()
        expires = int(self._interval) + 10
        self._lock_acquired = self.connection.set(
                key, now, ex=expires, nx=True)
        return self._lock_acquired

    def remove_lock(self):
        """
        Remove acquired lock.
        """
        key = '%s_lock' % self.scheduler_key

        if self._lock_acquired:
            self.connection.delete(key)

    def _install_signal_handlers(self):
        """
        Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        def stop(signum, frame):
            """
            Register scheduler's death and exit
            and remove previously acquired lock and exit.
            """
            self.log.info('Shutting down RQ scheduler...')
            self.register_death()
            self.remove_lock()
            raise SystemExit()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)

    def _create_job(self, func, args=None, kwargs=None, commit=True,
                    result_ttl=None, ttl=None, id=None, description=None,
                    queue_name=None, timeout=None, meta=None):
        """
        Creates an RQ job and saves it to Redis. The job is assigned to the
        given queue name if not None else it is assigned to scheduler queue by
        default.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        job = self.job_class.create(
                func, args=args, connection=self.connection,
                kwargs=kwargs, result_ttl=result_ttl, ttl=ttl, id=id,
                description=description, timeout=timeout, meta=meta)
        if queue_name:
            job.origin = queue_name
        else:
            job.origin = self.queue_name

        if commit:
            job.save()
        return job

    def enqueue_at(self, scheduled_time, func, *args, **kwargs):
        """
        Pushes a job to the scheduler queue. The scheduled queue is a Redis sorted
        set ordered by timestamp - which in this case is job's scheduled execution time.

        All args and kwargs are passed onto the job, except for the following kwarg
        keys (which affect the job creation itself):
        - timeout
        - job_id
        - job_ttl
        - job_result_ttl
        - job_description

        Usage:

        from datetime import datetime
        from redis import Redis
        from rq.scheduler import Scheduler

        from foo import func

        redis = Redis()
        scheduler = Scheduler(queue_name='default', connection=redis)
        scheduler.enqueue_at(datetime(2020, 1, 1), func, 'argument', keyword='argument')
        """
        timeout = kwargs.pop('timeout', None)
        job_id = kwargs.pop('job_id', None)
        job_ttl = kwargs.pop('job_ttl', None)
        job_result_ttl = kwargs.pop('job_result_ttl', None)
        job_description = kwargs.pop('job_description', None)
        meta = kwargs.pop('meta', None)
        queue_name = kwargs.pop('queue_name', None)

        job = self._create_job(func, args=args, kwargs=kwargs, timeout=timeout,
                               id=job_id, result_ttl=job_result_ttl, ttl=job_ttl,
                               description=job_description, meta=meta, queue_name=queue_name)
        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(scheduled_time)})
        return job

    def enqueue_in(self, time_delta, func, *args, **kwargs):
        """
        Similar to ``enqueue_at``, but accepts a timedelta instead of datetime object.
        The job's scheduled execution time will be calculated by adding the timedelta
        to datetime.utcnow().
        """
        timeout = kwargs.pop('timeout', None)
        job_id = kwargs.pop('job_id', None)
        job_ttl = kwargs.pop('job_ttl', None)
        job_result_ttl = kwargs.pop('job_result_ttl', None)
        job_description = kwargs.pop('job_description', None)
        meta = kwargs.pop('meta', None)
        queue_name = kwargs.pop('queue_name', None)

        job = self._create_job(func, args=args, kwargs=kwargs, timeout=timeout,
                               id=job_id, result_ttl=job_result_ttl, ttl=job_ttl,
                               description=job_description, meta=meta, queue_name=queue_name)
        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(datetime.utcnow() + time_delta)})
        return job

    def schedule(self, scheduled_time, func, args=None, kwargs=None,
                 interval=None, repeat=None, result_ttl=None, ttl=None,
                 timeout=None, id=None, description=None, queue_name=None,
                 meta=None):
        """
        Schedule a job to be periodically executed, at a certain interval.
        """
        # Set result_ttl to -1 for periodic jobs, if result_ttl not specified
        if interval is not None and result_ttl is None:
            result_ttl = -1
        job = self._create_job(func, args=args, kwargs=kwargs, commit=False,
                               result_ttl=result_ttl, ttl=ttl, id=id,
                               description=description, queue_name=queue_name,
                               timeout=timeout, meta=meta)

        if interval is not None:
            job.meta['interval'] = int(interval)
        if repeat is not None:
            job.meta['repeat'] = int(repeat)
        if repeat and interval is None:
            raise ValueError("Can't repeat a job without interval argument")
        job.save()
        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(scheduled_time)})
        return job

    def cron(self, cron_string, func, args=None, kwargs=None, repeat=None,
             queue_name=None, id=None, timeout=None, description=None, meta=None):
        """
        Schedule a cronjob
        """
        scheduled_time = get_next_scheduled_time(cron_string)

        # Set result_ttl to -1, as jobs scheduled via cron are periodic ones.
        # Otherwise the job would expire after 500 sec.
        job = self._create_job(func, args=args, kwargs=kwargs, commit=False,
                               result_ttl=-1, id=id, queue_name=queue_name,
                               description=description, timeout=timeout, meta=meta)

        job.meta['cron_string'] = cron_string

        if repeat is not None:
            job.meta['repeat'] = int(repeat)

        job.save()

        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(scheduled_time)})
        return job

    def cancel(self, job):
        """
        Pulls a job from the scheduler queue. This function accepts either a
        job_id or a job instance.
        """
        if isinstance(job, self.job_class):
            self.connection.zrem(self.scheduled_jobs_key, job.id)
        else:
            self.connection.zrem(self.scheduled_jobs_key, job)

    def __contains__(self, item):
        """
        Returns a boolean indicating whether the given job instance or job id
        is scheduled for execution.
        """
        job_id = item
        if isinstance(item, self.job_class):
            job_id = item.id
        return self.connection.zscore(self.scheduled_jobs_key, job_id) is not None

    def change_execution_time(self, job, date_time):
        """
        Change a job's execution time.
        """
        with self.connection.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(self.scheduled_jobs_key)
                    if pipe.zscore(self.scheduled_jobs_key, job.id) is None:
                        raise ValueError('Job not in scheduled jobs queue')
                    pipe.zadd(self.scheduled_jobs_key, {job.id: to_unix(date_time)})
                    break
                except WatchError:
                    # If job is still in the queue, retry otherwise job is already executed
                    # so we raise an error
                    if pipe.zscore(self.scheduled_jobs_key, job.id) is None:
                        raise ValueError('Job not in scheduled jobs queue')
                    continue

    def count(self, until=None):
        """
        Returns the total number of jobs that are scheduled for all queues.
        This function accepts datetime, timedelta instances as well as
        integers representing epoch values.
        """

        until = rationalize_until(until)
        return self.connection.zcount(self.scheduled_jobs_key, 0, until)

    def get_jobs(self, until=None, with_times=False, offset=None, length=None):
        """
        Returns a iterator of job instances that will be queued until the given
        time. If no 'until' argument is given all jobs are returned.

        If with_times is True, a list of tuples consisting of the job instance
        and it's scheduled execution time is returned.

        If offset and length are specified, a slice of the list starting at the
        specified zero-based offset of the specified length will be returned.

        If either of offset or length is specified, then both must be, or
        an exception will be raised.
        """
        def epoch_to_datetime(epoch):
            return from_unix(float(epoch))

        until = rationalize_until(until)
        job_ids = self.connection.zrangebyscore(self.scheduled_jobs_key, 0,
                                                until, withscores=with_times,
                                                score_cast_func=epoch_to_datetime,
                                                start=offset, num=length)
        if not with_times:
            job_ids = zip(job_ids, repeat(None))
        for job_id, sched_time in job_ids:
            job_id = job_id.decode('utf-8')
            try:
                job = self.job_class.fetch(job_id, connection=self.connection)
            except NoSuchJobError:
                # Delete jobs that aren't there from scheduler
                self.cancel(job_id)
                continue
            if with_times:
                yield (job, sched_time)
            else:
                yield job

    def get_jobs_to_queue(self, with_times=False):
        """
        Returns a list of job instances that should be queued
        (score lower than current timestamp).
        If with_times is True a list of tuples consisting of the job instance and
        it's scheduled execution time is returned.
        """
        return self.get_jobs(to_unix(datetime.utcnow()), with_times=with_times)

    def get_queue_for_job(self, job):
        """
        Returns a queue to put job into.
        """
        key = '{0}{1}'.format(self.queue_class.redis_queue_namespace_prefix,
                              job.origin)
        return self.queue_class.from_queue_key(
                key, connection=self.connection, job_class=self.job_class)

    def enqueue_job(self, job):
        """
        Move a scheduled job to a queue. In addition, it also does puts the job
        back into the scheduler if needed.
        """
        self.log.debug('Pushing {0} to {1}'.format(job.id, job.origin))

        interval = job.meta.get('interval', None)
        repeat = job.meta.get('repeat', None)
        cron_string = job.meta.get('cron_string', None)

        # If job is a repeated job, decrement counter
        if repeat:
            job.meta['repeat'] = int(repeat) - 1

        queue = self.get_queue_for_job(job)
        queue.enqueue_job(job)
        self.connection.zrem(self.scheduled_jobs_key, job.id)

        if interval:
            # If this is a repeat job and counter has reached 0, don't repeat
            if repeat is not None:
                if job.meta['repeat'] == 0:
                    return
            self.connection.zadd(self.scheduled_jobs_key,
                                  {job.id: to_unix(datetime.utcnow()) + int(interval)})
        elif cron_string:
            # If this is a repeat job and counter has reached 0, don't repeat
            if repeat is not None:
                if job.meta['repeat'] == 0:
                    return
            self.connection.zadd(self.scheduled_jobs_key,
                                  {job.id: to_unix(get_next_scheduled_time(cron_string))})

    def enqueue_jobs(self):
        """
        Move scheduled jobs into queues.
        """
        self.log.debug('Checking for scheduled jobs')

        jobs = self.get_jobs_to_queue()
        for job in jobs:
            self.enqueue_job(job)

        # Refresh scheduler key's expiry
        self.connection.expire(self.scheduler_key, int(self._interval) + 10)
        return jobs

    def run(self, burst=False):
        """
        Periodically check whether there's any job that should be put in the queue (score
        lower than current time).
        """

        self.register_birth()
        self._install_signal_handlers()

        try:
            while True:
                self.log.debug("Entering run loop")

                start_time = time.time()
                if self.acquire_lock():
                    self.enqueue_jobs()
                    self.remove_lock()

                    if burst:
                        self.log.info('RQ scheduler done, quitting')
                        break
                else:
                    self.log.warning('Lock already taken - skipping run')

                # Time has already elapsed while enqueuing jobs, so don't wait too long.
                seconds_elapsed_since_start = time.time() - start_time
                seconds_until_next_scheduled_run = self._interval - seconds_elapsed_since_start
                # ensure we have a non-negative number
                if seconds_until_next_scheduled_run > 0:
                    self.log.debug("Sleeping %.2f seconds" % seconds_until_next_scheduled_run)
                    time.sleep(seconds_until_next_scheduled_run)
        finally:
            self.remove_lock()
            self.register_death()
