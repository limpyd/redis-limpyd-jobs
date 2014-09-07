from __future__ import unicode_literals
from __future__ import division
from future.builtins import int

from datetime import datetime, timedelta
import time


def datetime_to_score(dt):
    """
    Convert the given datetime object to be usable as a zset score
    """
    return time.mktime(dt.timetuple()) + dt.microsecond / 1000000


def get_delta(value):
    """
    Return a timedelta object based on the value which can be a timedelta
    or a number of seconds (int or float).
    Raise an exception in all other cases.
    """
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    elif isinstance(value, timedelta):
        return value
    raise Exception('Invalid delta')


def compute_delayed_until(delayed_for=None, delayed_until=None):
    """
    Return a datetime object based on either the `delayed_for` argument
    (int/float/timedelta), which wille be added to the current datetime
    (`datetime.utcnow`), or the `delayed_until` one which must be a
    datetime object.
    Raise a `ValueError` exception if both are set, or a invalid type is
    used.
    Returns None if both arguments are None.
    """
    if delayed_until:
        if delayed_for:
            raise ValueError('delayed_until and delayed_for arguments are exclusive')
        if not isinstance(delayed_until, datetime):
            raise ValueError('Invalid delayed_until argument: must be a datetime object')

    if delayed_for:
        try:
            delayed_until = datetime.utcnow() + get_delta(delayed_for)
        except Exception:
            raise ValueError('Invalid delayed_for argument: must be an int, a float or a timedelta object')

    return delayed_until


try:
    from importlib import import_module  # pragma: no cover
except ImportError:  # pragma: no cover
    def import_module(module_uri):
        """
        Replacement to import_module from importlib for python 2.6
        """
        return __import__(module_uri, {}, {}, [''])  # pragma: no cover


def import_class(class_uri):
    """
    Import a class by string 'from.path.module.class'
    """

    parts = class_uri.split('.')
    class_name = parts.pop()
    module_uri = '.'.join(parts)

    try:
        module = import_module(module_uri)
    except ImportError as e:
        # maybe we are still in a module, test going up one level
        try:
            module = import_class(module_uri)
        except Exception:
            # if failure raise the original exception
            raise e

    return getattr(module, class_name)


def total_seconds(td):
    # Keep backward compatibility with Python 2.6 which doesn't have
    # this method
    if hasattr(td, 'total_seconds'):  # pragma: no cover
        return td.total_seconds()  # pragma: no cover
    else:
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6  # pragma: no cover
