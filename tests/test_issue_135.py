"""Test for issue #135 using doctest.

You should make sure to set your PYTHONPATH appropriately when testing.
For example, if you have the development version of rq_scheduler installed
at /x/y/rq-scheduler, then you could test by typing something like

$ PYTHONPATH=/x/y/rq-scheduler python3 /x/y/rq-scheduler/tests/test_issue_135

>>> from datetime import datetime, timedelta
>>> from redis import Redis
>>> from rq_scheduler import Scheduler
>>> from rq_scheduler import Scheduler
>>> import test_issue_135

>>> redis = Redis()
>>> my_scheduler = Scheduler(connection=redis, queue_name='my_queue')
>>> start_time = datetime.utcnow() + timedelta(seconds=5)
>>> job = my_scheduler.enqueue_at(
...     start_time, test_issue_135.say_hi, 'test', result_ttl=0, 
...     queue_name='tst_q')
>>> job.result_ttl
0
>>> job.origin
'tst_q'


"""

import doctest

def say_hi():
    print('hi')
    return 'hi'

if __name__ == '__main__':
    doctest.testmod()
    print('Finished tests')
    