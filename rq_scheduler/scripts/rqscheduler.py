#!/usr/bin/env python

import argparse
import sys
import os

from redis import Redis
from rq_scheduler.scheduler import Scheduler

from rq.logutils import setup_loghandlers
from rq.cli import helpers


KEY_HOST = "host"
KEY_PORT = "port"
KEY_DB = "db"
KEY_PASSWORD = "password"
KEY_URL = "url"

def main():

    # set up minimal argparser to get -c option
    parser = argparse.ArgumentParser(
        add_help=False  # help will be picked up later when we redfine parser
    )
    parser.add_argument('-c', "--config", help='Use an rq config file')
    args, remaining_argv = parser.parse_known_args()

    # config, pass 1: read environment vars
    config = {
        KEY_HOST : os.environ.get('RQ_REDIS_HOST', 'localhost'),
        KEY_PORT : int(os.environ.get('RQ_REDIS_PORT', 6379)),
        KEY_DB : int(os.environ.get('RQ_REDIS_DB', 0)),
        KEY_PASSWORD : os.environ.get('RQ_REDIS_PASSWORD'),
        KEY_URL : os.environ.get('RQ_REDIS_URL')
    }

    # config, pass 2: read config file
    if args.config:
        # bit of a hack, this, but does allow helpers.read_config_file to work...
        sys.path.insert( 0, os.path.dirname(os.path.realpath(args.config)) )
        rq_config = helpers.read_config_file( args.config )
        # map rq settings to our own config dict
        config[KEY_URL] = rq_config.get("REDIS_URL", config[KEY_URL])
        config[KEY_HOST] = rq_config.get("REDIS_HOST", config[KEY_HOST])
        config[KEY_PORT] = rq_config.get("REDIS_PORT", config[KEY_PORT])
        config[KEY_DB] = rq_config.get("REDIS_DB", config[KEY_DB])
        config[KEY_PASSWORD] = rq_config.get("REDIS_PASSWORD",config[KEY_PASSWORD])

    # config, pass 3: read commandline args. overwrites any other config.
    parser = argparse.ArgumentParser(
        parents=[parser]  # inherit from existing parser
    )
    parser.add_argument('-H', '--host', default=config[KEY_HOST], help="Redis host")
    parser.add_argument('-p', '--port', default=config[KEY_PORT], type=int, help="Redis port number")
    parser.add_argument('-d', '--db', default=config[KEY_DB], type=int, help="Redis database")
    parser.add_argument('-P', '--password', default=config[KEY_PASSWORD], help="Redis password")
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Show more output')
    parser.add_argument('--url', '-u', default=config[KEY_URL]
        , help='URL describing Redis connection details. \
            Overrides other connection arguments if supplied.')
    parser.add_argument('-i', '--interval', default=60.0, type=float
        , help="How often the scheduler checks for new jobs to add to the \
            queue (in seconds, can be floating-point for more precision).")
    parser.add_argument('--path', default='.', help='Specify the import path.')
    parser.add_argument('--pid', help='A filename to use for the PID file.', metavar='FILE')
    
    args = parser.parse_args( remaining_argv )
    
    if args.path:
        sys.path = args.path.split(':') + sys.path
    
    if args.pid:
        pid = str(os.getpid())
        filename = args.pid
        with open(filename, 'w') as f:
            f.write(pid)
    
    if args.url is not None:
        connection = Redis.from_url(args.url)
    else:
        connection = Redis(args.host, args.port, args.db, args.password)

    if args.verbose:
        level = 'DEBUG'
    else:
        level = 'INFO'
    setup_loghandlers(level)

    scheduler = Scheduler(connection=connection, interval=args.interval)
    scheduler.run()

if __name__ == '__main__':
    main()
