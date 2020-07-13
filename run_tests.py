#!/usr/bin/env python

import socket
import sys
import unittest

def is_redis_ready():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 6379))
    return result == 0


def main():
    if not is_redis_ready():
        print("Redis server is not running.", file=sys.stderr)
        return 1

    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().discover("tests"))
    result = unittest.TextTestRunner().run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
