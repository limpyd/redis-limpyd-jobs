from datetime import datetime, timedelta
from dateutil.parser import parse
import time
from redis.client import Lock

from limpyd import fields
from limpyd.contrib import database, collection
from limpyd_extensions import related

from limpyd_jobs import STATUSES, LimpydJobsException

__all__ = ('BaseJobsModel', 'Queue', 'Job', 'Error')


def datetime_to_score(dt):
    """
    Convert the given datetime object to be usable as a zset score
    """
    return time.mktime(dt.timetuple()) + dt.microsecond / 1000000.0


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


class BaseJobsModel(related.RelatedModel):
    collection_manager = collection.ExtendedCollectionManager
    database = database.PipelineDatabase()
    namespace = 'jobs'
    abstract = True
    cacheable = False

    def set_fields(self, **fields):
        """
        Set many fields using the proxy setter for each of them.
        """
        for field_name, value in fields.iteritems():
            field = getattr(self, field_name)
            field.proxy_set(value)


class Queue(BaseJobsModel):
    name = fields.InstanceHashField(indexable=True)
    priority = fields.InstanceHashField(indexable=True, default=0)  # the higher, the better
    waiting = fields.ListField()
    success = fields.ListField()
    errors = fields.ListField()
    delayed = fields.SortedSetField()

    @classmethod
    def get_queue(cls, name, priority=0, **fields_if_new):
        """
        Get, or create, and return the wanted queue.
        If the queue is created, fields in fields_if_new will be set for the new
        queue.
        """
        queue, created = cls.get_or_connect(
            name=name,
            priority=priority,
        )
        if created and fields_if_new:
            queue.set_fields(**fields_if_new)

        return queue

    def delay_job(self, job_pk, delayed_until):
        """
        Add the job to the delayed list (zset) of the queue.
        """
        timestamp = datetime_to_score(delayed_until)
        self.delayed.zadd(timestamp, job_pk)

    def enqueue_job(self, job_pk, prepend=False):
        """
        Add the job to the waiting list, at the end (it's a fifo list). If
        `prepend` is True, add it at the beginning of the list.
        """
        push_method = getattr(self.waiting, 'lpush' if prepend else 'rpush')
        push_method(job_pk)

    @classmethod
    def get_all_by_priority(cls, name):
        """
        Return all the queues with the given name, sorted by priorities (higher
        priority first)
        """
        return cls.collection(name=name).sort(by='-priority').instances()

    @classmethod
    def get_waiting_keys(cls, name):
        """
        Return a list of all queue waiting keys, to use with blpop
        """
        return [col.waiting.key for col in cls.get_all_by_priority(name)]

    @classmethod
    def count_waiting_jobs(cls, name):
        """
        Return the number of all jobs waiting in queues with the given name
        """
        return sum([col.waiting.llen() for col in cls.get_all_by_priority(name)])

    @classmethod
    def count_delayed_jobs(cls, name):
        """
        Return the number of all delayed jobs in queues with the given name
        """
        return sum([col.delayed.zcard() for col in cls.get_all_by_priority(name)])

    @property
    def first_delayed(self):
        """
        Return the first entry in the delayed zset (a tuple with the job's pk
        and the score of the zset, which it's delayed time as a timestamp)
        Returns None if no delayed jobs
        """
        entries = self.delayed.zrange(0, 0, withscores=True)
        return entries[0] if entries else None

    @property
    def first_delayed_time(self):
        """
        Get the timestamp representation of the first delayed job to be ready.
        """
        # get the first job which will be ready
        first_entry = self.first_delayed

        return first_entry[1] if first_entry else None

    def requeue_delayed_jobs(self, job_model):
        """
        Put all delayed jobs that are now ready, back in the queue waiting list
        """
        lock_key = self.make_key(
            self._name,
            self.pk.get(),
            "requeue_all_delayed_ready_jobs",
        )
        connection = self.get_connection()

        if connection.exists('lock_key'):
            # if locked, a worker is already on it, don't wait and exit
            return

        with Lock(connection, lock_key, timeout=60):

            # stop here if we know we have nothing
            first_delayed_time = self.first_delayed_time
            if not first_delayed_time:
                return None

            # get when we are :)
            now_timestamp = datetime_to_score(datetime.utcnow())

            # the first job will be ready later, and so the other ones too, then
            # abort
            if float(first_delayed_time) > now_timestamp:
                return None

            while True:
                # get the first entry
                first_entry = self.first_delayed

                # no first entry, another worker took all from us !
                if not first_entry:
                    break

                # split into vars for readability
                job_pk, delayed_until = first_entry

                # if the date of the job is in the future, another work took the
                # job we wanted, so we let this job here and stop the loop as we
                # know (its a zset sorted by date) that no other jobs are ready
                if delayed_until > now_timestamp:
                    break

                # remove the entry we just got from the delayed ones
                self.delayed.zrem(job_pk)

                # and add it to the waiting queue
                job = job_model.get(job_pk)
                job.status.hset(STATUSES.WAITING)
                self.enqueue_job(job_pk)


class Job(BaseJobsModel):
    identifier = fields.InstanceHashField(indexable=True)  # ex: "myobj:123:update"
    status = fields.InstanceHashField(indexable=True)  # see STATUSES constants
    priority = fields.InstanceHashField(indexable=True, default=0)
    added = fields.InstanceHashField()
    start = fields.InstanceHashField()
    end = fields.InstanceHashField()
    tries = fields.InstanceHashField()
    delayed_until = fields.InstanceHashField()

    queue_model = Queue

    @classmethod
    def add_job(cls, identifier, queue_name, priority=0, queue_model=None,
                prepend=False, delayed_for=None, delayed_until=None,
                **fields_if_new):
        """
        Add a job to a queue.
        If this job already exists, check it's current priority. If its higher
        than the new one, don't touch it, else move the job to the wanted queue.
        Before setting/moving the job to the queue, check for a `delayed_for`
        (int/foat/timedelta) or `delayed_until` (datetime) argument to see if
        it must be delayed instead of queued.
        If the job is created, fields in fields_if_new will be set for the new
        job.
        Finally return the job.
        """

        # check for delayed_for/delayed_until arguments
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

        # create the job or get an existing one
        job, created = cls.get_or_connect(identifier=identifier, status=STATUSES.WAITING)

        # if the job already exists, and we want a higher priority or move it,
        # start by updating it
        if not created:
            current_priority = int(job.priority.hget() or 0)
            # if the job has a higher priority, or don't need to be moved,
            # don't move it
            if not prepend and current_priority >= priority:
                return job

            # cancel it temporarily, we'll set it as waiting later
            job.status.hset(STATUSES.CANCELED)

            # remove it from the current queue, we'll add it to the new one later
            if queue_model is None:
                queue_model = cls.queue_model
            current_queue = queue_model.get_queue(queue_name, current_priority)
            current_queue.waiting.lrem(0, job.pk.get())

        elif fields_if_new:
            job.set_fields(added=str(datetime.utcnow()), **fields_if_new)

        # add the job to the queue
        job.enqueue_or_delay(queue_name, priority, delayed_until, prepend, queue_model)

        return job

    @property
    def duration(self):
        """
        If the start and end times of the job are defined, return a timedelta,
        else return None
        """
        try:
            start, end = self.hmget('start', 'end')
            return parse(end) - parse(start)
        except:
            return None

    def requeue(self, queue_name, priority=None, requeue_delay_delta=None, queue_model=None):
        """
        Requeue the job in the given queue if it has previously failed
        """
        # we can only requeue a job that raised an error
        if self.status.hget() != STATUSES.ERROR:
            raise LimpydJobsException('Job cannot be requeued if not in ERROR status')

        self.hdel('start', 'end')

        if priority is None:
            priority = self.priority.hget()

        delayed_until = None
        if requeue_delay_delta:
            delayed_until = datetime.utcnow() + get_delta(requeue_delay_delta)

        self.enqueue_or_delay(queue_name, priority, delayed_until, queue_model=queue_model)

    def enqueue_or_delay(self, queue_name, priority, delayed_until=None, prepend=False, queue_model=None):
        """
        Will enqueue or delay the job depending of the delayed_until.
        """
        fields = {'priority': priority}
        in_the_future = delayed_until and delayed_until > datetime.utcnow()

        if in_the_future:
            fields['delayed_until'] = str(delayed_until)
            fields['status'] = STATUSES.DELAYED
        else:
            self.delayed_until.delete()
            fields['status'] = STATUSES.WAITING

        self.hmset(**fields)

        if queue_model is None:
            queue_model = self.queue_model
        queue = queue_model.get_queue(queue_name, priority)

        if in_the_future:
            queue.delay_job(self.pk.get(), delayed_until)
        else:
            queue.enqueue_job(self.pk.get(), prepend)


class Error(BaseJobsModel):
    job_pk = fields.InstanceHashField(indexable=True)
    identifier = fields.InstanceHashField(indexable=True)
    queue_name = fields.InstanceHashField(indexable=True)
    date = fields.InstanceHashField(indexable=True)
    time = fields.InstanceHashField()
    type = fields.InstanceHashField(indexable=True)
    code = fields.InstanceHashField(indexable=True)
    message = fields.InstanceHashField()
    traceback = fields.InstanceHashField()

    @classmethod
    def add_error(cls, queue_name, job, error, when=None, trace=None,
                                                        **additional_fields):
        """
        Add a new error in redis.
        `job` is a job which generated the error
        `queue_name` is the name of the queue where the error arrived
        `error` is an exception, which can has a code (better if it is)
        `when` is the datetime of the error, utcnow will be used if not defined
        `trace` is the traceback generated by the exception, may be null
        The new created instance is returned, with additional_fields set for
        aubclasses.
        """
        if when is None:
            when = datetime.utcnow()

        fields = dict(
            queue_name=queue_name,
            job_pk=job.pk.get(),
            identifier=getattr(job, '_cached_identifier', job.identifier.hget()),
            date=str(when.date()),
            time=str(when.time()),
            message=str(error),
        )

        try:
            # exception can be a class (should not, but just in case...)
            fields['type'] = error.__name__
        except AttributeError:
            # or excetion is an instance
            fields['type'] = error.__class__.__name__

        error_code = getattr(error, 'code', None)
        if error_code is not None:
            fields['code'] = error_code

        if trace:
            fields['traceback'] = trace

        error = cls(**fields)

        if additional_fields:
            error.set_fields(**additional_fields)

        return error

    @property
    def datetime(self):
        """
        Property which return a real datetime object based on the date and time
        fields
        """
        date, time = self.hmget('date', 'time')
        return parse('%s %s' % (date, time))
