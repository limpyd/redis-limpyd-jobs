from __future__ import unicode_literals

from .version import *


class STATUSES(dict):
    """
    All available job's statuses.
    To use this way:
        job.status.hset(STATUSES.SUCCESS)  # or STATUSES['SUCCESS']
    Available statuses: DELAYED, WAITING, RUNNING, SUCCESS, ERROR, CANCELED
    Use the `by_value` to get the key from a value:
        STATUSES.by_value(job.status.hget())
        >> 'SUCCESS'
    """
    def __getattr__(self, name):
        return self[name]

    def by_value(self, value, default=None):
        """
        Returns the key for the given value
        """
        try:
            return [k for k, v in self.items() if v == value][0]
        except IndexError:
            if default is not None:
                return default
            raise ValueError('%s' % value)


STATUSES = STATUSES(
        WAITING='w',
        DELAYED='d',
        RUNNING='r',
        SUCCESS='s',
        ERROR='e',
        CANCELED='c',
    )


class LimpydJobsException(Exception):
    pass


class ConfigurationException(LimpydJobsException):
    pass


# the imports below are to ease import for users of the module
from .models import Queue, Job, Error
from .workers import Worker
