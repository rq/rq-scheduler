import calendar
import crontab
import dateutil.tz
import dateutil.rrule
import re

from datetime import datetime, timedelta
import logging

from rq.logutils import ColorizingStreamHandler


# from_unix from times.from_unix()
def from_unix(string):
    """Convert a unix timestamp into a utc datetime"""
    return datetime.utcfromtimestamp(float(string))


# to_unix from times.to_unix()
def to_unix(dt):
    """Converts a datetime object to unixtime"""
    return calendar.timegm(dt.utctimetuple())


def get_next_scheduled_time(cron_string, use_local_timezone=False):
    """Calculate the next scheduled time by creating a crontab object
    with a cron string"""
    now = datetime.now()
    cron = crontab.CronTab(cron_string)
    next_time = cron.next(now=now, return_datetime=True)
    tz = dateutil.tz.tzlocal() if use_local_timezone else dateutil.tz.UTC
    return next_time.astimezone(tz)


def get_next_rrule_scheduled_time(rrule_string):
    """Calculate the next scheduled time by creating a rrule object
    with a rrule string"""
    timezone = dateutil.tz.UTC
    ruleset = dateutil.rrule.rrulestr(rrule_string, forceset=True)
    any_occurence = None
    for occur in ruleset:
        any_occurence = occur
        break
    if any_occurence is None:
        return None
    if any_occurence.tzinfo is None:
        now = datetime.now()
    else:
        now = datetime.now(tz=timezone)
    next_occurence = ruleset.after(now)
    if next_occurence is None:
        return None
    return next_occurence.astimezone(timezone)


def setup_loghandlers(level='INFO'):
    logger = logging.getLogger('rq_scheduler.scheduler')
    if not logger.handlers:
        logger.setLevel(level)
        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                      datefmt='%H:%M:%S')
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def rationalize_until(until=None):
    """
    Rationalizes the `until` argument used by other functions. This function
    accepts datetime and timedelta instances as well as integers representing
    epoch values.
    """
    if until is None:
        until = "+inf"
    elif isinstance(until, datetime):
        until = to_unix(until)
    elif isinstance(until, timedelta):
        until = to_unix((datetime.utcnow() + until))
    return until
