import calendar
import crontab
import dateutil.tz
import logging

from typing import Union
from datetime import datetime
from datetime import timedelta

from rq.logutils import ColorizingStreamHandler


def from_unix(string: str) -> datetime:
    """Converts a unix timestamp into a datetime object

    Args:
        string (str): The unix timestamp

    Returns:
        datetime: The datetime object
    """    
    return datetime.utcfromtimestamp(float(string))


def to_unix(dt: datetime) -> int:
    """Converts a datetime object to unixtime

    Args:
        dt (datetime): The datetime object to convert

    Returns:
        timestamp (int): The unix timestamp
    """
    return calendar.timegm(dt.utctimetuple())


def get_next_scheduled_time(cron_string: str, use_local_timezone: bool = False) -> datetime:
    """Calculate the next scheduled time by creating a crontab object
    with a cron string

    Args:
        cron_string (str): The cron string
        use_local_timezone (bool, optional): Whether to use the local timezone. Defaults to False.

    Returns:
        datetime: The next scheduled time as datetime object
    """
    now = datetime.now()
    cron = crontab.CronTab(cron_string)
    next_time = cron.next(now=now, return_datetime=True)
    tz = dateutil.tz.tzlocal() if use_local_timezone else dateutil.tz.UTC
    return next_time.astimezone(tz)


def setup_loghandlers(level: str = 'INFO'):
    """Setup the log handlers for the scheduler

    Args:
        level (str, optional): The log level. Defaults to 'INFO'.
    """    
    logger = logging.getLogger('rq_scheduler.scheduler')
    if not logger.handlers:
        logger.setLevel(level)
        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                      datefmt='%H:%M:%S')
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def rationalize_until(until: Union[None, datetime, timedelta, int] = None) -> Union[str, int]:
    """Rationalizes the `until` argument used by other functions.
    This function accepts datetime and timedelta instances as well as integers representing epoch values.

    Args:
        until (Union[None, datetime, timedelta], optional): The until argument. Defaults to None.

    Returns:
        Union[str, int]: The rationalized until argument
    """
    if until is None:
        until = "+inf"
    elif isinstance(until, datetime):
        until = to_unix(until)
    elif isinstance(until, timedelta):
        until = to_unix((datetime.utcnow() + until))
    return until
