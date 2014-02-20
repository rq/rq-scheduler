import calendar
from datetime import datetime

# from_unix from times.from_unix()
def from_unix(string):
    """Convert a unix timestamp into a utc datetime"""
    return datetime.utcfromtimestamp(float(string))


# to_unix from times.to_unix()
def to_unix(dt):
    """Converts a datetime object to unixtime"""
    return calendar.timegm(dt.utctimetuple())
