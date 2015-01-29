import unittest
from redis import StrictRedis
from rq import push_connection, pop_connection


def find_empty_redis_database():
    """Tries to connect to a random Redis database (starting from 4), and
    will use/connect it when no keys are in there.
    """
    for dbnum in range(4, 17):
        testconn = StrictRedis(db=dbnum)
        empty = len(testconn.keys('*')) == 0
        if empty:
            return testconn
    assert False, 'No empty Redis database found to run tests in.'


class RQTestCase(unittest.TestCase):
    """Base class to inherit test cases from for RQ.

    It sets up the Redis connection (available via self.testconn), turns off
    logging to the terminal and flushes the Redis database before and after
    running each test.

    Also offers assertQueueContains(queue, that_func) assertion method.
    """

    @classmethod
    def setUpClass(cls):
        # Set up connection to Redis
        testconn = find_empty_redis_database()
        push_connection(testconn)

        # Store the connection (for sanity checking)
        cls.testconn = testconn

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.testconn.flushdb()

    def tearDown(self):
        # Flush afterwards
        self.testconn.flushdb()

    # Implement assertIsNotNone for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertIsNotNone'):
        def assertIsNotNone(self, value, *args):
            self.assertNotEqual(value, None, *args)

    # Implement assertIn for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertIn'):
        def assertIn(self, first, second, msg=None):
            self.assertTrue(first in second, msg=msg)

    # Implement assertNotIn for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertNotIn'):
        def assertNotIn(self, first, second, msg=None):
            self.assertTrue(first not in second, msg=msg)

    # Implement assertIsInstance for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertIsInstance'):
        def assertIsInstance(self, obj, cls, msg=None):
            self.assertTrue(isinstance(obj, cls), msg=msg)

    @classmethod
    def tearDownClass(cls):

        # Pop the connection to Redis
        testconn = pop_connection()
        assert testconn == cls.testconn, 'Wow, something really nasty ' \
                'happened to the Redis connection stack. Check your setup.'

# for python < 2.7, which doesn't have setUpClass
if not hasattr(unittest.TestCase, 'setUpClass'):
    RQTestCase.setUpClass()
