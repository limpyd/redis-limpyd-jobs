"A queue/jobs system based on redis-limpyd, a redis orm (sort of) in python"
VERSION = (0, 0, 1)

__author__ = 'Stephane "Twidi" Angel'
__contact__ = "s.angel@twidi.com"
__homepage__ = "https://github.com/twidi/redis-limpyd-jobs"
__version__ = ".".join(map(str, VERSION))


class STATUSES():
    """
    All available job's statuses.
    To use this way:
        job.status.hset(STATUSES.SUCCESS)
    Available statuses: WAITING, RUNNING, SUCCESS, ERROR, CANCELED
    """
    WAITING = 'w'
    RUNNING = 'r'
    SUCCESS = 's'
    ERROR = 'e'
    CANCELED = 'c'
