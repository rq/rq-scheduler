from datetime import datetime, timedelta
import os
import signal
import time
from threading import Thread

from rq import Queue
from rq.compat import as_text
from rq.job import Job
from rq_scheduler import Scheduler
from rq_scheduler.utils import to_unix, from_unix, get_next_scheduled_time

from tests import RQTestCase


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def tl(l):
    return [as_text(i) for i in l]


def simple_addition(x, y, z):
    return x + y + z


class TestScheduler(RQTestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()
        self.scheduler = Scheduler(connection=self.testconn)

    def test_acquire_lock(self):
        """
        When scheduler acquires a lock, besides creating a key, it should
        also set an expiry that's a few seconds longer than it's polling
        interval so it automatically expires if scheduler is unexpectedly
        terminated.
        """
        key = '%s_lock' % Scheduler.scheduler_key
        self.assertNotIn(key, tl(self.testconn.keys('*')))
        scheduler = Scheduler(connection=self.testconn, interval=20)
        self.assertTrue(scheduler.acquire_lock())
        self.assertIn(key, tl(self.testconn.keys('*')))
        self.assertEqual(self.testconn.ttl(key), 30)
        scheduler.remove_lock()
        self.assertNotIn(key, tl(self.testconn.keys('*')))

    def test_no_two_schedulers_acquire_lock(self):
        """
        Ensure that no two schedulers can acquire the lock at the
        same time. When removing the lock, only the scheduler which
        originally acquired the lock can remove the lock.
        """
        key = '%s_lock' % Scheduler.scheduler_key
        self.assertNotIn(key, tl(self.testconn.keys('*')))
        scheduler1 = Scheduler(connection=self.testconn, interval=20)
        scheduler2 = Scheduler(connection=self.testconn, interval=20)
        self.assertTrue(scheduler1.acquire_lock())
        self.assertFalse(scheduler2.acquire_lock())
        self.assertIn(key, tl(self.testconn.keys('*')))
        scheduler2.remove_lock()
        self.assertIn(key, tl(self.testconn.keys('*')))
        scheduler1.remove_lock()
        self.assertNotIn(key, tl(self.testconn.keys('*')))

    def test_create_job(self):
        """
        Ensure that jobs are created properly.
        """
        job = self.scheduler._create_job(say_hello, args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job, job_from_queue)
        self.assertEqual(job_from_queue.func, say_hello)

    def test_create_job_with_ttl(self):
        """
        Ensure that TTL is passed to RQ.
        """
        job = self.scheduler._create_job(say_hello, ttl=2, args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(2, job_from_queue.ttl)

    def test_create_job_with_id(self):
        """
        Ensure that ID is passed to RQ.
        """
        job = self.scheduler._create_job(say_hello, id='id test', args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual('id test', job_from_queue.id)

    def test_create_job_with_description(self):
        """
        Ensure that description is passed to RQ.
        """
        job = self.scheduler._create_job(say_hello, description='description', args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual('description', job_from_queue.description)

    def test_create_job_with_timeout(self):
        """
        Ensure that timeout is passed to RQ.
        """
        timeout = 13
        job = self.scheduler._create_job(say_hello, timeout=13, args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(timeout, job_from_queue.timeout)

    def test_job_not_persisted_if_commit_false(self):
        """
        Ensure jobs are only saved to Redis if commit=True.
        """
        job = self.scheduler._create_job(say_hello, commit=False)
        self.assertEqual(self.testconn.hgetall(job.key), {})

    def test_create_scheduled_job(self):
        """
        Ensure that scheduled jobs are put in the scheduler queue with the right score
        """
        scheduled_time = datetime.utcnow()
        job = self.scheduler.enqueue_at(scheduled_time, say_hello)
        self.assertEqual(job, Job.fetch(job.id, connection=self.testconn))
        self.assertIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         to_unix(scheduled_time))

    def test_create_job_with_meta(self):
        """
        Ensure that meta information on the job is passed to rq
        """
        expected = {'say': 'hello'}
        job = self.scheduler._create_job(say_hello, meta=expected)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(expected, job_from_queue.meta)

    def test_enqueue_at_sets_timeout(self):
        """
        Ensure that a job scheduled via enqueue_at can be created with
        a custom timeout.
        """
        timeout = 13
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello, timeout=timeout)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.timeout, timeout)

    def test_enqueue_at_sets_job_id(self):
        """
        Ensure that a job scheduled via enqueue_at can be created with
        a custom job id.
        """
        job_id = 'test_id'
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello, job_id=job_id)
        self.assertEqual(job.id, job_id)

    def test_enqueue_at_sets_job_ttl(self):
        """
        Ensure that a job scheduled via enqueue_at can be created with a custom job ttl.
        """
        job_ttl = 123456789
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello, job_ttl=job_ttl)
        self.assertEqual(job.ttl, job_ttl)

    def test_enqueue_at_sets_job_result_ttl(self):
        """
        Ensure that a job scheduled via enqueue_at can be created with a custom result ttl.
        """
        job_result_ttl = 1234567890
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello, job_result_ttl=job_result_ttl)
        self.assertEqual(job.result_ttl, job_result_ttl)

    def test_enqueue_at_sets_meta(self):
        """
        Ensure that a job scheduled via enqueue_at can be created with a custom meta.
        """
        meta = {'say': 'hello'}
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello, meta=meta)
        self.assertEqual(job.meta, meta)

    def test_enqueue_in(self):
        """
        Ensure that jobs have the right scheduled time.
        """
        right_now = datetime.utcnow()
        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        self.assertIn(job.id,
                      tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         to_unix(right_now + time_delta))
        time_delta = timedelta(hours=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         to_unix(right_now + time_delta))

    def test_enqueue_in_sets_timeout(self):
        """
        Ensure that a job scheduled via enqueue_in can be created with
        a custom timeout.
        """
        timeout = 13
        job = self.scheduler.enqueue_in(timedelta(minutes=1), say_hello, timeout=timeout)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.timeout, timeout)

    def test_enqueue_in_sets_job_id(self):
        """
        Ensure that a job scheduled via enqueue_in can be created with
        a custom job id.
        """
        job_id = 'test_id'
        job = self.scheduler.enqueue_in(timedelta(minutes=1), say_hello, job_id=job_id)
        self.assertEqual(job.id, job_id)

    def test_enqueue_in_sets_job_ttl(self):
        """
        Ensure that a job scheduled via enqueue_in can be created with a custom job ttl.
        """
        job_ttl = 123456789
        job = self.scheduler.enqueue_in(timedelta(minutes=1), say_hello, job_ttl=job_ttl)
        self.assertEqual(job.ttl, job_ttl)

    def test_enqueue_in_sets_job_result_ttl(self):
        """
        Ensure that a job scheduled via enqueue_in can be created with a custom result ttl.
        """
        job_result_ttl = 1234567890
        job = self.scheduler.enqueue_in(timedelta(minutes=1), say_hello, job_result_ttl=job_result_ttl)
        self.assertEqual(job.result_ttl, job_result_ttl)

    def test_enqueue_in_sets_meta(self):
        """
        Ensure that a job scheduled via enqueue_in sets meta.
        """
        meta = {'say': 'hello'}
        job = self.scheduler.enqueue_in(timedelta(minutes=1), say_hello, meta=meta)
        self.assertEqual(job.meta, meta)

    def test_count(self):
        now = datetime.utcnow()
        self.scheduler.enqueue_at(now, say_hello)
        self.assertEqual(self.scheduler.count(), 1)

        future_time = now + timedelta(hours=1)
        future_test_time = now + timedelta(minutes=59, seconds=59)

        self.scheduler.enqueue_at(future_time, say_hello)

        self.assertEqual(self.scheduler.count(timedelta(minutes=59, seconds=59)), 1)
        self.assertEqual(self.scheduler.count(future_test_time), 1)
        self.assertEqual(self.scheduler.count(), 2)

    def test_get_jobs(self):
        """
        Ensure get_jobs() returns all jobs until the specified time.
        """
        now = datetime.utcnow()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs(now))
        future_time = now + timedelta(hours=1)
        job = self.scheduler.enqueue_at(future_time, say_hello)
        self.assertIn(job, self.scheduler.get_jobs(timedelta(hours=1, seconds=1)))
        self.assertIn(job, [j[0] for j in self.scheduler.get_jobs(with_times=True)])
        self.assertIsInstance(list(self.scheduler.get_jobs(with_times=True))[0][1], datetime)
        self.assertNotIn(job, self.scheduler.get_jobs(timedelta(minutes=59, seconds=59)))

    def test_get_jobs_slice(self):
        """
        Ensure get_jobs() returns the appropriate slice of all jobs using offset and length.
        """
        now = datetime.utcnow()
        future_time = now + timedelta(hours=1)
        future_test_time = now + timedelta(minutes=59, seconds=59)

        # Schedule each job a second later than the previous job,
        # otherwise Redis will return jobs that have the same scheduled time in
        # lexicographical order (not the order in which we enqueued them)
        now_jobs = [self.scheduler.enqueue_at(now + timedelta(seconds=x), say_hello)
                    for x in range(15)]
        future_jobs = [self.scheduler.enqueue_at(future_time + timedelta(seconds=x), say_hello)
                       for x in range(15)]

        expected_slice = now_jobs[5:] + future_jobs[:10]   # last 10 from now_jobs and first 10 from future_jobs
        expected_until_slice = now_jobs[5:]                # last 10 from now_jobs

        jobs = self.scheduler.get_jobs()
        jobs_slice = self.scheduler.get_jobs(offset=5, length=20)
        jobs_until_slice = self.scheduler.get_jobs(future_test_time, offset=5, length=20)

        self.assertEqual(now_jobs + future_jobs, list(jobs))
        self.assertEqual(expected_slice, list(jobs_slice))
        self.assertEqual(expected_until_slice, list(jobs_until_slice))

    def test_get_jobs_to_queue(self):
        """
        Ensure that jobs scheduled the future are not queued.
        """
        now = datetime.utcnow()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs_to_queue())
        future_time = now + timedelta(hours=1)
        job = self.scheduler.enqueue_at(future_time, say_hello)
        self.assertNotIn(job, self.scheduler.get_jobs_to_queue())

    def test_enqueue_job(self):
        """
        When scheduled job is enqueued, make sure:
        - Job is removed from the sorted set of scheduled jobs
        - "enqueued_at" attribute is properly set
        - Job appears in the right queue
        - Queue is recognized by rq's Queue.all()
        """
        now = datetime.utcnow()
        queue_name = 'foo'
        scheduler = Scheduler(connection=self.testconn, queue_name=queue_name)

        job = scheduler.enqueue_at(now, say_hello)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job, tl(self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 10)))
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.enqueued_at is not None)
        queue = scheduler.get_queue_for_job(job)
        self.assertIn(job, queue.jobs)
        queue = Queue.from_queue_key('rq:queue:{0}'.format(queue_name))
        self.assertIn(job, queue.jobs)
        self.assertIn(queue, Queue.all())

    def test_enqueue_job_with_scheduler_queue(self):
        """
        Ensure that job is enqueued correctly when the scheduler is bound
        to a queue object and job queue name is not provided.
        """
        queue = Queue('foo', connection=self.testconn)
        scheduler = Scheduler(connection=self.testconn, queue=queue)
        job = scheduler._create_job(say_hello)
        scheduler_queue = scheduler.get_queue_for_job(job)
        self.assertEqual(queue, scheduler_queue)
        scheduler.enqueue_job(job)
        self.assertTrue(job.enqueued_at is not None)
        self.assertIn(job, queue.jobs)
        self.assertIn(queue, Queue.all())

    def test_enqueue_job_with_job_queue_name(self):
        """
        Ensure that job is enqueued correctly when queue_name is provided
        at job creation
        """
        queue = Queue('foo', connection=self.testconn)
        job_queue = Queue('job_foo', connection=self.testconn)
        scheduler = Scheduler(connection=self.testconn, queue=queue)
        job = scheduler._create_job(say_hello, queue_name='job_foo')
        self.assertEqual(scheduler.get_queue_for_job(job), job_queue)
        scheduler.enqueue_job(job)
        self.assertTrue(job.enqueued_at is not None)
        self.assertIn(job, job_queue.jobs)
        self.assertIn(job_queue, Queue.all())

    def test_job_membership(self):
        now = datetime.utcnow()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler)
        self.assertIn(job.id, self.scheduler)
        self.assertNotIn("non-existing-job-id", self.scheduler)

    def test_cancel_scheduled_job(self):
        """
        When scheduled job is canceled, make sure:
        - Job is removed from the sorted set of scheduled jobs
        """
        # schedule a job to be enqueued one minute from now
        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        # cancel the scheduled job and check that it's gone from the set
        self.scheduler.cancel(job)
        self.assertNotIn(job.id, tl(self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1)))

    def test_change_execution_time(self):
        """
        Ensure ``change_execution_time`` is called, ensure that job's score is updated
        """
        job = self.scheduler.enqueue_at(datetime.utcnow(), say_hello)
        new_date = datetime(2010, 1, 1)
        self.scheduler.change_execution_time(job, new_date)
        self.assertEqual(to_unix(new_date),
            self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id))
        self.scheduler.cancel(job)
        self.assertRaises(ValueError, self.scheduler.change_execution_time, job, new_date)

    def test_args_kwargs_are_passed_correctly(self):
        """
        Ensure that arguments and keyword arguments are properly saved to jobs.
        """
        job = self.scheduler.enqueue_at(datetime.utcnow(), simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = self.scheduler.enqueue_at(datetime.utcnow(), simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = self.scheduler.enqueue_at(datetime.utcnow(), simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = self.scheduler.enqueue_in(time_delta, simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = self.scheduler.enqueue_in(time_delta, simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

    def test_interval_and_repeat_persisted_correctly(self):
        """
        Ensure that interval and repeat attributes are correctly saved.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=10, repeat=11)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.meta['interval'], 10)
        self.assertEqual(job_from_queue.meta['repeat'], 11)

    def test_crontab_persisted_correctly(self):
        """
        Ensure that crontab attribute gets correctly saved in Redis.
        """
        # create a job that runs one minute past each whole hour
        job = self.scheduler.cron("1 * * * *", say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.meta['cron_string'], "1 * * * *")

        # get the scheduled_time and convert it to a datetime object
        unix_time = self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id)
        datetime_time = from_unix(unix_time)

        # check that minute=1, seconds=0, and is within an hour
        assert datetime_time.minute == 1
        assert datetime_time.second == 0
        assert datetime_time - datetime.utcnow() < timedelta(hours=1)

    def test_crontab_sets_timeout(self):
        """
        Ensure that a job scheduled via crontab can be created with
        a custom timeout.
        """
        timeout = 13
        job = self.scheduler.cron("1 * * * *", say_hello, timeout=timeout)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.timeout, timeout)

    def test_crontab_sets_id(self):
        """
        Ensure that a job scheduled via crontab can be created with
        a custom id
        """
        job_id = "hello-job-id"
        job = self.scheduler.cron("1 * * * *", say_hello, id=job_id)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_id, job_from_queue.id)

    def test_crontab_sets_default_result_ttl(self):
        """
        Ensure that a job scheduled via crontab gets proper default
        result_ttl (-1) periodic tasks.
        """
        job = self.scheduler.cron("1 * * * *", say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(-1, job_from_queue.result_ttl)

    def test_crontab_sets_description(self):
        """
        Ensure that a job scheduled via crontab can be created with
        a custom description
        """
        description = 'test description'
        job = self.scheduler.cron("1 * * * *", say_hello, description=description)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(description, job_from_queue.description)

    def test_repeat_without_interval_raises_error(self):
        # Ensure that an error is raised if repeat is specified without interval
        def create_job():
            self.scheduler.schedule(datetime.utcnow(), say_hello, repeat=11)
        self.assertRaises(ValueError, create_job)

    def test_job_with_intervals_get_rescheduled(self):
        """
        Ensure jobs with interval attribute are put back in the scheduler
        """
        time_now = datetime.utcnow()
        interval = 10
        job = self.scheduler.schedule(time_now, say_hello, interval=interval)
        self.scheduler.enqueue_job(job)
        self.assertIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         to_unix(time_now) + interval)

    def test_job_with_interval_can_set_meta(self):
        """
        Ensure that jobs with interval attribute can be created with meta
        """
        time_now = datetime.utcnow()
        interval = 10
        meta = {'say': 'hello'}
        job = self.scheduler.schedule(time_now, say_hello, interval=interval, meta=meta)
        self.scheduler.enqueue_job(job)
        self.assertEqual(job.meta, meta)

    def test_job_with_crontab_get_rescheduled(self):
        # Create a job with a cronjob_string
        job = self.scheduler.cron("1 * * * *", say_hello)

        # current unix_time
        old_next_scheduled_time = self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id)

        # change crontab
        job.meta['cron_string'] = "2 * * * *"

        # enqueue the job
        self.scheduler.enqueue_job(job)

        self.assertIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))

        # check that next scheduled time has changed
        self.assertNotEqual(old_next_scheduled_time,
                            self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id))

        # check that new next scheduled time is set correctly
        expected_next_scheduled_time = to_unix(get_next_scheduled_time("2 * * * *"))
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         expected_next_scheduled_time)

    def test_job_with_repeat(self):
        """
        Ensure jobs with repeat attribute are put back in the scheduler
        X (repeat) number of times
        """
        time_now = datetime.utcnow()
        interval = 10
        # If job is repeated once, the job shouldn't be put back in the queue
        job = self.scheduler.schedule(time_now, say_hello, interval=interval, repeat=1)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))

        # If job is repeated twice, it should only be put back in the queue once
        job = self.scheduler.schedule(time_now, say_hello, interval=interval, repeat=2)
        self.scheduler.enqueue_job(job)
        self.assertIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))

    def test_missing_jobs_removed_from_scheduler(self):
        """
        Ensure jobs that don't exist when queued are removed from the scheduler.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello)
        job.cancel()
        list(self.scheduler.get_jobs_to_queue())
        self.assertIn(job.id, tl(self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1)))
        job.delete()
        list(self.scheduler.get_jobs_to_queue())
        self.assertNotIn(job.id, tl(self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1)))

    def test_periodic_jobs_sets_result_ttl(self):
        """
        Ensure periodic jobs set result_ttl to infinite.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, -1)

    def test_periodic_jobs_sets_ttl(self):
        """
        Ensure periodic jobs sets correctly ttl.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5, ttl=4)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.ttl, 4)

    def test_periodic_jobs_sets_meta(self):
        """
        Ensure periodic jobs sets correctly meta.
        """
        meta = {'say': 'hello'}
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5, meta=meta)
        self.assertEqual(meta, job.meta)

    def test_periodic_job_sets_id(self):
        """
        Ensure that ID is passed to RQ by schedule.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5, id='id test')
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual('id test', job.id)

    def test_periodic_job_sets_description(self):
        """
        Ensure that description is passed to RQ by schedule.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5, description='description')
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual('description', job.description)

    def test_run(self):
        """
        Check correct signal handling in Scheduler.run().
        """
        def send_stop_signal():
            """
            Sleep for 1 second, then send a INT signal to ourself, so the
            signal handler installed by scheduler.run() is called.
            """
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)
        thread = Thread(target=send_stop_signal)
        thread.start()
        self.assertRaises(SystemExit, self.scheduler.run)
        thread.join()

    def test_run_burst(self):
        """
        Check burst mode of Scheduler.run().
        """
        now = datetime.utcnow()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs_to_queue())
        self.assertEqual(len(list(self.scheduler.get_jobs())), 1)
        self.scheduler.run(burst=True)
        self.assertEqual(len(list(self.scheduler.get_jobs())), 0)

    def test_scheduler_w_o_explicit_connection(self):
        """
        Ensure instantiating Scheduler w/o explicit connection works.
        """
        s = Scheduler()
        self.assertEqual(s.connection, self.testconn)

    def test_small_float_interval(self):
        """
        Test that scheduler accepts 'interval' of type float, less than 1 second.
        """
        key = Scheduler.scheduler_key
        lock_key = '%s_lock' % Scheduler.scheduler_key
        self.assertNotIn(key, tl(self.testconn.keys('*')))
        scheduler = Scheduler(connection=self.testconn, interval=0.1)   # testing interval = 0.1 second
        self.assertEqual(scheduler._interval, 0.1)

        #acquire lock
        self.assertTrue(scheduler.acquire_lock())
        self.assertIn(lock_key, tl(self.testconn.keys('*')))
        self.assertEqual(self.testconn.ttl(lock_key), 10)  # int(0.1) + 10 = 10

        #enqueue a job
        now = datetime.utcnow()
        job = scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs_to_queue())
        self.assertEqual(len(list(self.scheduler.get_jobs())), 1)

        #remove the lock
        scheduler.remove_lock()

        #test that run works with the small floating-point interval
        def send_stop_signal():
            """
            Sleep for 1 second, then send a INT signal to ourself, so the
            signal handler installed by scheduler.run() is called.
            """
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)
        thread = Thread(target=send_stop_signal)
        thread.start()
        self.assertRaises(SystemExit, scheduler.run)
        thread.join()

        #all jobs must have been scheduled during 1 second
        self.assertEqual(len(list(scheduler.get_jobs())), 0)

    def test_get_queue_for_job_with_job_queue_name(self):
        """
        Tests that scheduler gets the correct queue for the job when
        queue_name is provided.
        """
        queue = Queue('scheduler_foo', connection=self.testconn)
        job_queue = Queue('job_foo', connection=self.testconn)
        scheduler = Scheduler(connection=self.testconn, queue=queue)
        job = scheduler._create_job(say_hello, queue_name='job_foo')
        self.assertEqual(scheduler.get_queue_for_job(job), job_queue)

    def test_get_queue_for_job_without_job_queue_name(self):
        """
        Tests that scheduler gets the scheduler queue for the job
        when queue name is not provided for that job.
        """
        queue = Queue('scheduler_foo', connection=self.testconn)
        scheduler = Scheduler(connection=self.testconn, queue=queue)
        job = scheduler._create_job(say_hello)
        self.assertEqual(scheduler.get_queue_for_job(job), queue)
