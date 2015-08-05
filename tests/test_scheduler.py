import unittest
from datetime import datetime, timedelta
import os
import signal
import time
from threading import Thread

from rq import Queue
from rq.compat import as_text
from rq.job import Job
import warnings
from rq_scheduler import Scheduler
from rq_scheduler.utils import to_unix

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
    
    def test_birth_and_death_registration(self):
        """
        When scheduler registers it's birth, besides creating a key, it should
        also set an expiry that's a few seconds longer than it's polling
        interval so it automatically expires if scheduler is unexpectedly 
        terminated.
        """
        key = Scheduler.scheduler_key
        self.assertNotIn(key, tl(self.testconn.keys('*')))
        scheduler = Scheduler(connection=self.testconn, interval=20)
        scheduler.register_birth()
        self.assertIn(key, tl(self.testconn.keys('*')))
        self.assertEqual(self.testconn.ttl(key), 30)
        self.assertFalse(self.testconn.hexists(key, 'death'))
        self.assertRaises(ValueError, scheduler.register_birth)
        scheduler.register_death()
        self.assertTrue(self.testconn.hexists(key, 'death'))

    def test_create_job(self):
        """
        Ensure that jobs are created properly.
        """
        job = self.scheduler._create_job(say_hello, args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job, job_from_queue)
        self.assertEqual(job_from_queue.func, say_hello)

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
        self.assertIsInstance(self.scheduler.get_jobs(with_times=True)[0][1], datetime)
        self.assertNotIn(job, self.scheduler.get_jobs(timedelta(minutes=59, seconds=59)))
    
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

    def test_enqueue_is_deprecated(self):
        """
        Ensure .enqueue() throws a DeprecationWarning
        """
        with warnings.catch_warnings(record=True) as w:
            # Enable all warnings
            warnings.simplefilter("always")
            job = self.scheduler.enqueue(datetime.utcnow(), say_hello)
            self.assertEqual(1, len(w))
            self.assertEqual(w[0].category, DeprecationWarning)

    def test_enqueue_periodic(self):
        """
        Ensure .enqueue_periodic() throws a DeprecationWarning
        """
        with warnings.catch_warnings(record=True) as w:
            # Enable all warnings
            warnings.simplefilter("always")
            job = self.scheduler.enqueue_periodic(datetime.utcnow(), 1, None, say_hello)
            self.assertEqual(1, len(w))
            self.assertEqual(w[0].category, DeprecationWarning)

    def test_interval_and_repeat_persisted_correctly(self):
        """
        Ensure that interval and repeat attributes get correctly saved in Redis.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=10, repeat=11)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.meta['interval'], 10)
        self.assertEqual(job_from_queue.meta['repeat'], 11)

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

        # Now the same thing using enqueue_periodic
        job = self.scheduler.enqueue_periodic(time_now, interval, None, say_hello)
        self.scheduler.enqueue_job(job)
        self.assertIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         to_unix(time_now) + interval)

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

        time_now = datetime.utcnow()
        # Now the same thing using enqueue_periodic
        job = self.scheduler.enqueue_periodic(time_now, interval, 1, say_hello)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id,
            tl(self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)))

        # If job is repeated twice, it should only be put back in the queue once
        job = self.scheduler.enqueue_periodic(time_now, interval, 2, say_hello)
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
        self.scheduler.get_jobs_to_queue()
        self.assertNotIn(job.id, tl(self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1)))

    def test_periodic_jobs_sets_ttl(self):
        """
        Ensure periodic jobs set result_ttl to infinite.
        """
        job = self.scheduler.schedule(datetime.utcnow(), say_hello, interval=5)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, -1)

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
        self.assertNotIn(key, tl(self.testconn.keys('*')))
        scheduler = Scheduler(connection=self.testconn, interval=0.1)   # testing interval = 0.1 second
        self.assertEqual(scheduler._interval, 0.1)

        #register birth
        scheduler.register_birth()
        self.assertIn(key, tl(self.testconn.keys('*')))
        self.assertEqual(self.testconn.ttl(key), 10)  # int(0.1) + 10 = 10
        self.assertFalse(self.testconn.hexists(key, 'death'))

        #enqueue a job
        now = datetime.utcnow()
        job = scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs_to_queue())
        self.assertEqual(len(self.scheduler.get_jobs()), 1)

        #register death
        scheduler.register_death()

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
        self.assertEqual(len(scheduler.get_jobs()), 0)

    def test_check_stale_job(self):
        """
        Test check stale jobs works
        - create stale jobs
        - check them removed from scheduler
        - check them moved into stale queue
        """
        # 3 stale jobs
        stale_scheduled_time = datetime.utcnow() - timedelta(hours=3)
        stale_job_1 = self.scheduler.enqueue_at(stale_scheduled_time, say_hello)
        stale_scheduled_time = datetime.utcnow() - timedelta(hours=4)
        stale_job_2 = self.scheduler.enqueue_at(stale_scheduled_time, say_hello)
        stale_scheduled_time = datetime.utcnow() - timedelta(hours=5)
        stale_job_3 = self.scheduler.enqueue_at(stale_scheduled_time, say_hello)

        # 2 normal jobs
        scheduled_time = datetime.utcnow() + timedelta(hours=1)
        job_1 = self.scheduler.enqueue_at(scheduled_time, say_hello)
        scheduled_time = datetime.utcnow() + timedelta(hours=2)
        job_2 = self.scheduler.enqueue_at(scheduled_time, say_hello)

        # now number of all jobs should be 5
        jobs = self.scheduler.get_jobs()
        self.assertEqual(len(jobs), 5)

        # execute check_stale_job
        self.scheduler._check_stale_jobs()
        jobs = self.scheduler.get_jobs()
        # default is not to check stale job
        self.assertEqual(len(jobs), 5)

        # set max stale hours < 0, means to handle past jobs
        self.scheduler.max_stale_hours = -2
        self.scheduler._check_stale_jobs()
        # should be just 2
        jobs = self.scheduler.get_jobs()
        self.assertEqual(len(jobs), 2)
        # check stale job origin
        stale_job_1 = Job.fetch(stale_job_1.id, connection=self.testconn)
        stale_job_2 = Job.fetch(stale_job_2.id, connection=self.testconn)
        stale_job_3 = Job.fetch(stale_job_3.id, connection=self.testconn)
        job_1 = Job.fetch(job_1.id, connection=self.testconn)
        job_2 = Job.fetch(job_2.id, connection=self.testconn)

        self.assertEqual(stale_job_1.origin, self.scheduler.stale_queue_name)
        self.assertEqual(stale_job_2.origin, self.scheduler.stale_queue_name)
        self.assertEqual(stale_job_3.origin, self.scheduler.stale_queue_name)
        self.assertEqual(job_1.origin, self.scheduler.queue_name)
        self.assertEqual(job_2.origin, self.scheduler.queue_name)

        # check stale queue status
        stale_queue = self.scheduler._get_queue_by_name(self.scheduler.stale_queue_name)
        stale_job_ids = stale_queue.get_job_ids()
        self.assertIn(stale_job_1.id, stale_job_ids)
        self.assertIn(stale_job_2.id, stale_job_ids)
        self.assertIn(stale_job_3.id, stale_job_ids)
        self.assertNotIn(job_1.id, stale_job_ids)
        self.assertNotIn(job_2.id, stale_job_ids)
