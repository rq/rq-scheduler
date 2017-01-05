#!/usr/bin/env python

import argparse
import sys
import os

from redis import Redis
from rq_scheduler.scheduler import Scheduler

from rq_scheduler.utils import setup_loghandlers


def main():
    parser = argparse.ArgumentParser(description=(
        'Lists jobs in RQ scheduler.'))
    parser.add_argument(
        '-H', '--host', default=os.environ.get('RQ_REDIS_HOST', 'localhost'),
        help="Redis host")
    parser.add_argument(
        '-p', '--port', default=int(os.environ.get('RQ_REDIS_PORT', 6379)),
        type=int, help="Redis port number")
    parser.add_argument(
        '-d', '--db', default=int(os.environ.get('RQ_REDIS_DB', 0)),
        type=int, help="Redis database")
    parser.add_argument(
        '-P', '--password', default=os.environ.get('RQ_REDIS_PASSWORD'),
        help="Redis password")
    parser.add_argument('--verbose', '-v', action='store_true',
                        default=False, help='Show more output')    
    parser.add_argument('--url', '-u', default=os.environ.get('RQ_REDIS_URL')
        , help='URL describing Redis connection details. \
            Overrides other connection arguments if supplied.')
    parser.add_argument('--path', default='.', help='Specify the import path.')

    args = parser.parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    if args.url is not None:
        connection = Redis.from_url(args.url)
    else:
        connection = Redis(args.host, args.port, args.db, args.password)

    if args.verbose:
        level = 'DEBUG'
    else:
        level = 'INFO'
    setup_loghandlers(level)

    show_jobs(connection)


def show_jobs(connection):
    "Main function to query the scheduler and show the jobs."
    
    scheduler = Scheduler(connection=connection)
    job_list = scheduler.get_jobs(with_times=True)
    msg = ['Found %i scheduled jobs:' % len(job_list)]
    boundary = '-'*35
    for my_job, my_time in job_list:
        msg.append('%s\n%s :' % (boundary, my_job))
        msg.append('   time = %s' % (my_time))
        msg.append('   is_queued = %s, is_started = %s, status = %s' % (
            my_job.is_queued, my_job.is_started, my_job.status))
        msg.append('   meta = %s' % str(my_job.meta))
        my_result = str(my_job.result)
        msg.append('   result = %s' % (
            my_result if len(my_result) < 20 else (my_result[:17] + '...')))
        msg.append(boundary + '\n')
    print('\n'.join(msg))

if __name__ == '__main__':
    main()
