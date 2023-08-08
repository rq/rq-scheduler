import logging
import signal
import time
import os

from typing import Dict
from typing import Generator
from typing import Optional
from typing import Type
from typing import List
from typing import Union
from typing import Tuple
from typing import Any
from typing import Callable
from uuid import uuid4
from redis import WatchError
from redis import Connection
from datetime import datetime
from datetime import timedelta
from itertools import repeat

from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.job import Callback
from rq.queue import Queue
from rq.types import FunctionReferenceType
from rq.types import JobDependencyType
from rq.utils import backend_class, import_attribute

from .utils import from_unix, to_unix, get_next_scheduled_time, rationalize_until


logger = logging.getLogger(__name__)


class Scheduler(object):
    redis_scheduler_namespace_prefix = 'rq:scheduler_instance:'
    scheduler_key = 'rq:scheduler'
    scheduler_lock_key = 'rq:scheduler_lock'
    scheduled_jobs_key = 'rq:scheduler:scheduled_jobs'
    queue_class = Queue
    job_class = Job

    def __init__(
        self,
        queue_name: str = 'default',
        queue: Optional['Queue'] = None,
        interval: int = 60,
        connection: Optional['Connection'] = None,
        job_class: Optional[Type['Job']] = None,
        queue_class: Optional[Type['Queue']] = None,
        name: Optional[str] = None
    ):
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
        self.queue_class_name = None
        if isinstance(queue_class, str):
            self.queue_class_name = queue_class
        self.queue_class = backend_class(self, 'queue_class', override=queue_class)
        self.name = name or uuid4().hex

    @property
    def key(self):
        """Returns the schedulers Redis hash key."""
        return self.redis_scheduler_namespace_prefix + self.name

    @property
    def pid(self):
        """The current process ID."""
        return os.getpid()

    def register_birth(self):
        """Registers the scheduler birth.
        It will raise a `ValueError` if there's already an active scheduler.
        If the scheduler is already registered, it will update the heartbeat.

        The registration process sets the scheduler key to expire a few seconds after polling interval
        This way, the key will automatically expire if scheduler quits unexpectedly

        Raises:
            ValueError: If there's already an active scheduler.
        """
        self.log.info('Registering birth')
        if self.connection.exists(self.key) and \
                not self.connection.hexists(self.key, 'death'):
            raise ValueError("There's already an active RQ scheduler named: {0!r}".format(self.name))

        key = self.key
        now = time.time()

        with self.connection.pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            p.expire(key, int(self._interval) + 10)
            p.execute()

    def register_death(self):
        """Registers its own death.
        This will set the `death` key to the current timestamp,
        and set the scheduler key to expire in 60 seconds.
        """
        self.log.info('Registering death')
        with self.connection.pipeline() as p:
            p.hset(self.key, 'death', time.time())
            p.expire(self.key, 60)
            p.execute()

    def acquire_lock(self) -> bool:
        """Acquires a lock before scheduling jobs to prevent another scheduler
        from scheduling jobs at the same time.

        Returns:
            bool: True if lock is acquired. False otherwise.
        """
        key = self.scheduler_lock_key
        now = time.time()
        expires = int(self._interval) + 10
        self._lock_acquired = self.connection.set(key, now, ex=expires, nx=True)
        return self._lock_acquired

    def remove_lock(self):
        """Removes the lock if it was acquired.
        Deletes the key and sets the internal lock property (_lock_acquired) to False.
        """
        key = self.scheduler_lock_key
        if self._lock_acquired:
            self.connection.delete(key)
            self._lock_acquired = False
            self.log.debug('{}: Lock Removed'.format(self.key))

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
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

    def _create_job(
        self,
        func: FunctionReferenceType,
        args: Union[List[Any], Optional[Tuple]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        commit: bool = True,
        result_ttl: Optional[int] = None,
        ttl: Optional[int] = None,
        id: Optional[str] = None,
        description: Optional[str] = None,
        queue_name: Optional[str] = None,
        timeout: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        depends_on: Optional[JobDependencyType] = None,
        on_success: Optional[Union['Callback', Callable[..., Any]]] = None,
        on_failure: Optional[Union['Callback', Callable[..., Any]]] = None,
    ) -> 'Job':
        """Creates an RQ job and saves it to Redis.
        The job is assigned to the given queue name if not None
        else it is assigned to scheduler queue by default.

        Args:
            func (FunctionReferenceType): The reference to the function to be executed.
            args (Union[List[Any], Optional[Tuple]], optional): Function args. Defaults to None.
            kwargs (Optional[Dict[str, Any]], optional): Function kwargs. Defaults to None.
            commit (bool, optional): Whether to commit. Defaults to True.
            result_ttl (Optional[int], optional): The result TTL. Defaults to None.
            ttl (Optional[int], optional): The job TTL. Defaults to None.
            id (Optional[str], optional): The job ID. Defaults to None.
            description (Optional[str], optional): The job description. Defaults to None.
            queue_name (Optional[str], optional): The queue name to use. Defaults to None.
            timeout (Optional[int], optional): The job timeout. Defaults to None.
            meta (Optional[Dict[str, Any]], optional): Metadata about the job. Defaults to None.
            depends_on (Optional[JobDependencyType], optional): Job dependencies. Defaults to None.
            on_success (Optional[Union[Callback, Callable[..., Any]]], optional): OnSucess Callback. Defaults to None.
            on_failure (Optional[Union[Callback, Callable[..., Any]]], optional): OnFailure Callback. Defaults to None.

        Returns:
            Job: The created job instance.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        job = self.job_class.create(
            func, args=args, connection=self.connection,
            kwargs=kwargs, result_ttl=result_ttl, ttl=ttl, id=id,
            description=description, timeout=timeout, meta=meta,
            depends_on=depends_on,on_success=on_success,on_failure=on_failure,
        )
        if queue_name:
            job.origin = queue_name
        else:
            job.origin = self.queue_name

        if self.queue_class_name:
            job.meta["queue_class_name"] = self.queue_class_name

        if commit:
            job.save()
        return job

    def enqueue_at(self, scheduled_time: datetime, func: 'FunctionReferenceType', *args, **kwargs) -> 'Job':
        """
        Pushes a job to be execute at the given `scheduled_time` to the scheduler queue.
        The scheduled queue is a Redis sorted set ordered by timestamp
        which in this case is job's scheduled execution time.

        All *args and **kwargs are passed onto the job,
        except for the following kwarg keys (which affect the job creation itself):
            - timeout
            - job_id
            - job_ttl
            - job_result_ttl
            - job_description
            - depends_on
            - meta
            - queue_name
            - on_success
            - on_failure
            - at_front

        Example::

            ..code-block::python

                from datetime import datetime
                from redis import Redis
                from rq.scheduler import Scheduler

                from foo import func

                redis = Redis()
                scheduler = Scheduler(queue_name='default', connection=redis)
                scheduler.enqueue_at(datetime(2020, 1, 1), func, 'argument', keyword='argument')
        
        Args:
            scheduled_time (datetime): The scheduled time to execute the job.
            func (FunctionReferenceType): The reference to the function to be executed.
            *args: Function args.
            **kwargs: Function kwargs.

        Returns:
            Job: The created job instance.
        """
        timeout = kwargs.pop('timeout', None)
        job_id = kwargs.pop('job_id', None)
        job_ttl = kwargs.pop('job_ttl', None)
        job_result_ttl = kwargs.pop('job_result_ttl', None)
        job_description = kwargs.pop('job_description', None)
        depends_on = kwargs.pop('depends_on', None)
        meta = kwargs.pop('meta', None)
        queue_name = kwargs.pop('queue_name', None)
        on_success = kwargs.pop('on_success', None)
        on_failure = kwargs.pop('on_failure', None)
        at_front = kwargs.pop('at_front', None)

        job = self._create_job(func, args=args, kwargs=kwargs, timeout=timeout,
                               id=job_id, result_ttl=job_result_ttl, ttl=job_ttl,
                               description=job_description, meta=meta, queue_name=queue_name, depends_on=depends_on,
                               on_success=on_success, on_failure=on_failure)
        if at_front:
            job.enqueue_at_front = True
        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(scheduled_time)})
        return job

    def enqueue_in(self, time_delta: timedelta, func: 'FunctionReferenceType', *args, **kwargs) -> 'Job':
        """Similar to ``enqueue_at``, but accepts a timedelta instead of datetime object.
        The job's scheduled execution time will be calculated by adding the timedelta
        to datetime.utcnow().

        Args:
            time_delta (timedelta): The time delta to execute the job.
            func (FunctionReferenceType): The reference to the function to be executed.
            *args: Function args.
            **kwargs: Function kwargs.

        Returns:
            Job: The created job instance.
        """
        timeout = kwargs.pop('timeout', None)
        job_id = kwargs.pop('job_id', None)
        job_ttl = kwargs.pop('job_ttl', None)
        job_result_ttl = kwargs.pop('job_result_ttl', None)
        job_description = kwargs.pop('job_description', None)
        depends_on = kwargs.pop('depends_on', None)
        meta = kwargs.pop('meta', None)
        queue_name = kwargs.pop('queue_name', None)
        on_success = kwargs.pop('on_success', None)
        on_failure = kwargs.pop('on_failure', None)
        at_front = kwargs.pop('at_front', False)

        job = self._create_job(func, args=args, kwargs=kwargs, timeout=timeout,
                               id=job_id, result_ttl=job_result_ttl, ttl=job_ttl,
                               description=job_description, meta=meta, queue_name=queue_name,
                               depends_on=depends_on, on_success=on_success, on_failure=on_failure)
        if at_front:
            job.enqueue_at_front = True
        self.connection.zadd(self.scheduled_jobs_key,
                              {job.id: to_unix(datetime.utcnow() + time_delta)})
        return job

    def schedule(
        self,
        scheduled_time: datetime,
        func: 'FunctionReferenceType',
        args: Union[List[Any], Optional[Tuple]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        interval: Optional[Any] = None,
        repeat: Optional[Any] = None,
        result_ttl: Optional[int] = None,
        ttl: Optional[int] = None,
        timeout: Optional[int] = None,
        id: Optional[str] = None,
        description: Optional[str] = None,
        queue_name: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        depends_on: Optional[JobDependencyType] = None,
        on_success: Optional[Union['Callback', Callable[..., Any]]] = None,
        on_failure: Optional[Union['Callback', Callable[..., Any]]] = None,
        at_front: Optional[bool] = None
    ) -> 'Job':
        """Schedule a job to be periodically executed, at a certain interval.
        Set result_ttl to -1 for periodic jobs, if result_ttl not specified

        Args:
            scheduled_time (datetime): The scheduled time to execute the job.
            func (FunctionReferenceType): The reference to the function to be executed.
            args (Union[List[Any], Optional[Tuple]], optional): Function args. Defaults to None.
            kwargs (Optional[Dict[str, Any]], optional): Function kwargs. Defaults to None.
            interval (Optional[Any], optional): Interval to run. Defaults to None.
            repeat (Optional[Any], optional): Repeat. Defaults to None.
            result_ttl (Optional[int], optional): The Result TTL. Defaults to None.
            ttl (Optional[int], optional): The Job TTL. Defaults to None.
            timeout (Optional[int], optional): The Job Timeout. Defaults to None.
            id (Optional[str], optional): The Job ID. Defaults to None.
            description (Optional[str], optional): The Job description. Defaults to None.
            queue_name (Optional[str], optional): The queue name. Defaults to None.
            meta (Optional[Dict[str, Any]], optional): Job metadata. Defaults to None.
            depends_on (Optional[JobDependencyType], optional): Job dependencies. Defaults to None.
            on_success (Optional[Union[Callback, Callable[..., Any]]], optional): Job OnSuccess Callback. Defaults to None.
            on_failure (Optional[Union[Callback, Callable[..., Any]]], optional): Job OnFailure Callback. Defaults to None.
            at_front (Optional[bool], optional): Whether the job should be placed at the front of the queue. Defaults to None.

        Raises:
            ValueError: If repeat exists but interval is not defined.

        Returns:
            Job: The created job instance.
        """
        if interval is not None and result_ttl is None:
            result_ttl = -1
        job = self._create_job(func, args=args, kwargs=kwargs, commit=False,
                               result_ttl=result_ttl, ttl=ttl, id=id,
                               description=description, queue_name=queue_name,
                               timeout=timeout, meta=meta, depends_on=depends_on,
                               on_success=on_success, on_failure=on_failure)

        if interval is not None:
            job.meta['interval'] = int(interval)
        if repeat is not None:
            job.meta['repeat'] = int(repeat)
        if repeat and interval is None:
            raise ValueError("Can't repeat a job without interval argument")
        if at_front:
            job.enqueue_at_front = True
        job.save()
        self.connection.zadd(
            self.scheduled_jobs_key,
            {job.id: to_unix(scheduled_time)}
        )
        return job

    def cron(
        self,
        cron_string: str,
        func: 'FunctionReferenceType',
        args: Union[List[Any], Optional[Tuple]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        repeat: Optional[Any] = None,
        queue_name: Optional[str] = None,
        result_ttl: int = -1,
        ttl: Optional[int] = None,
        id: Optional[str] = None,
        timeout: Optional[int] = None,
        description: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        use_local_timezone: bool = False,
        depends_on: Optional[JobDependencyType] = None,
        on_success: Optional[Union['Callback', Callable[..., Any]]] = None,
        on_failure: Optional[Union['Callback', Callable[..., Any]]] = None,
        at_front: Optional[bool] = None
    ):
        """
        Schedule a cronjob.

        Args:
            cron_string (str): The cronstring.
            func (FunctionReferenceType): The reference to the function to be executed.
            args (Union[List[Any], Optional[Tuple]], optional): Function args. Defaults to None.
            kwargs (Optional[Dict[str, Any]], optional): Function kwargs. Defaults to None.
            repeat (Optional[Any], optional): Repeat. Defaults to None.
            queue_name (Optional[str], optional): The queue name. Defaults to None.
            result_ttl (int): The Result TTL. Defaults to -1.
            ttl (Optional[int], optional): The Job TTL. Defaults to None.
            id (Optional[str], optional): The Job ID. Defaults to None.
            timeout (Optional[int], optional): The Job Timeout. Defaults to None.
            description (Optional[str], optional): The Job description. Defaults to None.
            meta (Optional[Dict[str, Any]], optional): Job metadata. Defaults to None.
            use_local_timezone (bool): Whether to use the local timezone. Defaults to False.
            depends_on (Optional[JobDependencyType], optional): Job dependencies. Defaults to None.
            on_success (Optional[Union[Callback, Callable[..., Any]]], optional): Job OnSuccess Callback. Defaults to None.
            on_failure (Optional[Union[Callback, Callable[..., Any]]], optional): Job OnFailure Callback. Defaults to None.
            at_front (Optional[bool], optional): Whether the job should be placed at the front of the queue. Defaults to None.

        Returns:
            Job: The created job instance.
        """
        scheduled_time = get_next_scheduled_time(cron_string, use_local_timezone=use_local_timezone)
        job = self._create_job(func, args=args, kwargs=kwargs, commit=False,
                               result_ttl=result_ttl, ttl=ttl, id=id, queue_name=queue_name,
                               description=description, timeout=timeout, meta=meta, depends_on=depends_on,
                               on_success=on_success, on_failure=on_failure)

        job.meta['cron_string'] = cron_string
        job.meta['use_local_timezone'] = use_local_timezone

        if repeat is not None:
            job.meta['repeat'] = int(repeat)
        
        if at_front:
            job.enqueue_at_front = True

        job.save()
        self.connection.zadd(
            self.scheduled_jobs_key,
            {job.id: to_unix(scheduled_time)}
        )
        return job

    def cancel(self, job: Union['Job', str]):
        """Pulls a job from the scheduler queue.
        Accepts either a job_id or a job instance.

        Args:
            job (Union[Job, str]): The job to cancel, either a job instance or a job id.
        """
        if isinstance(job, self.job_class):
            self.connection.zrem(self.scheduled_jobs_key, job.id)
        else:
            self.connection.zrem(self.scheduled_jobs_key, job)

    def __contains__(self, item: Union['Job', str]) -> Optional[float]:
        """Returns a boolean indicating whether the given job instance or job id
        is scheduled for execution.

        Args:
            item (Union[Job, str]): The job to check, either a job instance or a job id.

        Returns:
            float: Score of the job if it exists, None otherwise.
        """
        job_id = item
        if isinstance(item, self.job_class):
            job_id = item.id
        return self.connection.zscore(self.scheduled_jobs_key, job_id) is not None

    def change_execution_time(self, job: 'Job', date_time: datetime):
        """Change a job's execution time.
        If job is still in the queue, retry
        otherwise job is already executed so we raise an error

        Args:
            job (Job): The job to change.
            date_time (datetime): The new execution time.

        Raises:
            ValueError: If job is not in scheduled jobs queue, or if job is already executed.
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
                    if pipe.zscore(self.scheduled_jobs_key, job.id) is None:
                        raise ValueError('Job not in scheduled jobs queue')
                    continue

    def count(self, until: Union[None, datetime, timedelta, int] = None) -> int:
        """Returns the total number of jobs that are scheduled for all queues.
        This function accepts datetime, timedelta instances as well as
        integers representing epoch values.

        Args:
            until (Union[None, datetime, timedelta, int], optional): Until. Defaults to None.

        Returns:
            int: The total number of jobs that are scheduled for all queues.
        """        
        until = rationalize_until(until)
        return self.connection.zcount(self.scheduled_jobs_key, 0, until)

    def get_jobs(
        self,
        until: Union[None, datetime, timedelta, int] = None,
        with_times: bool = False,
        offset: Optional[int] = None,
        length: Optional[int] = None
    ) -> Generator[Union[Job, Tuple[Job, datetime]], None, None]:
        """Returns a iterator of job instances that will be queued until the given
        time. If no 'until' argument is given all jobs are returned.

        If with_times is True, a list of tuples consisting of the job instance
        and it's scheduled execution time is returned.

        If offset and length are specified, a slice of the list starting at the
        specified zero-based offset of the specified length will be returned.

        If either of offset or length is specified, then both must be, or
        an exception will be raised.

        Args:
            until (Union[None, datetime, timedelta, int], optional): Time limit to get get jobs. Defaults to None.
            with_times (bool, optional): Whether to add the scheduled execution in the response. Defaults to False.
            offset (int, optional): Start list at. Defaults to None.
            length (int, optional): List length. Defaults to None.

        Yields:
            job: The job instance or a tuple of job instance and scheduled execution time.
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

    def get_jobs_to_queue(self, with_times: bool = False) -> Generator[Union[Job, Tuple[Job, datetime]], None, None]:
        """
        Returns a list of job instances that should be queued
        (score lower than current timestamp).
        If with_times is True a list of tuples consisting of the job instance and
        it's scheduled execution time is returned.

        Args:
            with_times (bool, optional): Whether to add the scheduled execution in the response. Defaults to False.

        Returns:
            list: List of job instances or a list of tuples of job instances and scheduled execution time.
        """
        return self.get_jobs(to_unix(datetime.utcnow()), with_times=with_times)

    def get_queue_for_job(self, job: 'Job') -> 'Queue':
        """
        Returns a queue to put job into.

        Args:
            job (Job): The job to get queue for.

        Returns:
            Queue: The queue to put job into.
        """
        key = '{0}{1}'.format(self.queue_class.redis_queue_namespace_prefix,
                              job.origin)
        if job.meta.get('queue_class_name'):
            queue_class = import_attribute(job.meta['queue_class_name'])
        else:
            queue_class = self.queue_class
        return queue_class.from_queue_key(key, connection=self.connection, job_class=self.job_class)

    def enqueue_job(self, job: 'Job'):
        """Move a scheduled job to a queue.
        In addition, it also does puts the job back into the scheduler if needed.
        If job is a repeated job, decrement counter.
        If job is a repeat job and counter has reached 0, don't repeat.

        Args:
            job (Job): The job to enqueue.
        """
        self.log.debug('Pushing {0}({1}) to {2}'.format(job.func_name, job.id, job.origin))

        interval = job.meta.get('interval', None)
        repeat = job.meta.get('repeat', None)
        cron_string = job.meta.get('cron_string', None)
        use_local_timezone = job.meta.get('use_local_timezone', None)

        if repeat:
            job.meta['repeat'] = int(repeat) - 1

        queue = self.get_queue_for_job(job)
        queue.enqueue_job(job, at_front=bool(job.enqueue_at_front))
        self.connection.zrem(self.scheduled_jobs_key, job.id)

        if interval:
            if repeat is not None:
                if job.meta['repeat'] == 0:
                    return
            self.connection.zadd(
                self.scheduled_jobs_key,
                {job.id: to_unix(datetime.utcnow()) + int(interval)}
            )
        elif cron_string:
            if repeat is not None:
                if job.meta['repeat'] == 0:
                    return
            next_scheduled_time = get_next_scheduled_time(cron_string, use_local_timezone=use_local_timezone)
            self.connection.zadd(
                self.scheduled_jobs_key,
                {job.id: to_unix(next_scheduled_time)}
            )

    def enqueue_jobs(self) -> Generator[Union[Job, Tuple[Job, datetime]], None, None]:
        """
        Move scheduled jobs into queues.

        Returns:
            jobs: List (a Generator) of jobs that were moved into queues.
        """
        self.log.debug('Checking for scheduled jobs')

        jobs = self.get_jobs_to_queue()
        for job in jobs:
            self.enqueue_job(job)
        
        return jobs

    def heartbeat(self):
        """Refreshes schedulers key, typically by extending the
        expiration time of the scheduler, effectively making this a "heartbeat"
        to not expire the scheduler until the timeout passes.
        """
        self.log.debug('{}: Sending a HeartBeat'.format(self.key))
        self.connection.expire(self.key, int(self._interval) + 10)

    def run(self, burst: bool = False):
        """
        Periodically check whether there's any job that should be put in the queue (score
        lower than current time).
        """
        self.register_birth()
        self._install_signal_handlers()

        try:
            while True:
                self.log.debug("Entering run loop")
                self.heartbeat()

                start_time = time.time()
                if self.acquire_lock():
                    self.log.debug('{}: Acquired Lock'.format(self.key))
                    self.enqueue_jobs()
                    self.heartbeat()
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
