from __future__ import unicode_literals
from future.builtins import str
from future.utils import iteritems
from past.builtins import basestring

from datetime import datetime

from dateutil.parser import parse

from limpyd import fields
from limpyd.database import Lock
from limpyd.contrib import database, collection
from limpyd_extensions import related

from limpyd_jobs import STATUSES, LimpydJobsException
from limpyd_jobs.utils import (
                               datetime_to_score,
                               compute_delayed_until,
                               import_class,
                               )

__all__ = ('BaseJobsModel', 'Queue', 'Job', 'Error')


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
        for field_name, value in iteritems(fields):
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
        queue_kwargs = {'name': name, 'priority': priority}
        retries = 0
        while retries < 10:
            retries += 1
            try:
                queue, created = cls.get_or_connect(**queue_kwargs)
            except IndexError:
                # Failure during the retrieval https://friendpaste.com/5U63a8aFuV44SEgQckgMP
                # => retry
                continue
            except ValueError:
                # more than one (race condition https://github.com/yohanboniface/redis-limpyd/issues/82 ?)
                try:
                    queue = cls.collection(**queue_kwargs).instances()[0]
                except IndexError:
                    # but no more now ?!
                    # => retry
                    continue
                else:
                    created = False

            # ok we have our queue, stop now
            break

        if created and fields_if_new:
            queue.set_fields(**fields_if_new)

        return queue

    def delay_job(self, job, delayed_until):
        """
        Add the job to the delayed list (zset) of the queue.
        """
        timestamp = datetime_to_score(delayed_until)
        self.delayed.zadd(timestamp, job.ident)

    def enqueue_job(self, job, prepend=False):
        """
        Add the job to the waiting list, at the end (it's a fifo list). If
        `prepend` is True, add it at the beginning of the list.
        """
        push_method = getattr(self.waiting, 'lpush' if prepend else 'rpush')
        push_method(job.ident)

    @staticmethod
    def _get_iterable_for_names(names):
        """
        Ensure that we have an iterable list of names, even if we have a single
        name
        """
        if isinstance(names, basestring):
            names = (names, )
        return names

    @classmethod
    def get_all(cls, names):
        """
        Return all queues for the given names (for all available priorities)
        """
        names = cls._get_iterable_for_names(names)

        queues = []
        for queue_name in names:
            queues.extend(cls.collection(name=queue_name).instances())

        return queues

    @classmethod
    def get_all_by_priority(cls, names):
        """
        Return all the queues with the given names, sorted by priorities (higher
        priority first), then by name
        """
        names = cls._get_iterable_for_names(names)

        queues = cls.get_all(names)

        # sort all queues by priority
        queues.sort(key=lambda q: int(q.priority.hget() or 0), reverse=True)

        return queues

    @classmethod
    def get_waiting_keys(cls, names):
        """
        Return a list of all queue waiting keys, to use with blpop
        """
        return [queue.waiting.key for queue in cls.get_all_by_priority(names)]

    @classmethod
    def count_waiting_jobs(cls, names):
        """
        Return the number of all jobs waiting in queues with the given names
        """
        return sum([queue.waiting.llen() for queue in cls.get_all(names)])

    @classmethod
    def count_delayed_jobs(cls, names):
        """
        Return the number of all delayed jobs in queues with the given names
        """
        return sum([queue.delayed.zcard() for queue in cls.get_all(names)])

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

    def requeue_delayed_jobs(self):
        """
        Put all delayed jobs that are now ready, back in the queue waiting list
        Return a list of failures
        """
        lock_key = self.make_key(
            self._name,
            self.pk.get(),
            "requeue_all_delayed_ready_jobs",
        )
        connection = self.get_connection()

        if connection.exists(lock_key):
            # if locked, a worker is already on it, don't wait and exit
            return []

        with Lock(connection, lock_key, timeout=60):

            # stop here if we know we have nothing
            first_delayed_time = self.first_delayed_time
            if not first_delayed_time:
                return []

            # get when we are :)
            now_timestamp = datetime_to_score(datetime.utcnow())

            # the first job will be ready later, and so the other ones too, then
            # abort
            if float(first_delayed_time) > now_timestamp:
                return []

            failures = []
            while True:
                # get the first entry
                first_entry = self.first_delayed

                # no first entry, another worker took all from us !
                if not first_entry:
                    break

                # split into vars for readability
                job_ident, delayed_until = first_entry

                # if the date of the job is in the future, another work took the
                # job we wanted, so we let this job here and stop the loop as we
                # know (its a zset sorted by date) that no other jobs are ready
                if delayed_until > now_timestamp:
                    break

                # remove the entry we just got from the delayed ones
                self.delayed.zrem(job_ident)

                # and add it to the waiting queue
                try:
                    job = Job.get_from_ident(job_ident)
                    if job.status.hget() == STATUSES.DELAYED:
                        job.status.hset(STATUSES.WAITING)
                    self.enqueue_job(job)
                except Exception as e:
                    failures.append((job_ident, '%s' % e))

            return failures


class Job(BaseJobsModel):
    identifier = fields.InstanceHashField(indexable=True)  # ex: "myobj:123:update"
    status = fields.InstanceHashField(indexable=True)  # see STATUSES constants
    priority = fields.InstanceHashField(indexable=True, default=0)
    added = fields.InstanceHashField()
    start = fields.InstanceHashField()
    end = fields.InstanceHashField()
    tries = fields.InstanceHashField()
    delayed_until = fields.InstanceHashField()
    queued = fields.InstanceHashField(indexable=True)  # '1' if queued
    cancel_on_error = fields.InstanceHashField()

    always_cancel_on_error = False

    queue_model = Queue
    queue_name = None

    @classmethod
    def get_model_repr(cls):
        """
        Return a string representation of the current class
        """
        return '%s.%s' % (cls.__module__, cls.__name__)

    @property
    def ident(self):
        """
        Return the string to use in  queues, include the job's class and its pk
        """
        return '%s:%s' % (self.get_model_repr(), self.pk.get())

    @classmethod
    def get_from_ident(self, ident):
        """
        Take a string as returned by get_ident and return a job,
        based on the class representation and the job's pk from the ident
        """
        model_repr, job_pk = ident.split(':', 1)
        klass = import_class(model_repr)
        return klass.get(job_pk)

    @classmethod
    def _get_queue_name(cls, queue_name=None):
        """
        Return the given queue_name if defined, else the class's one.
        If both are None, raise an Exception
        """
        if queue_name is None and cls.queue_name is None:
            raise LimpydJobsException("Queue's name not defined")
        if queue_name is None:
            return cls.queue_name
        return queue_name

    @classmethod
    def add_job(cls, identifier, queue_name=None, priority=0, queue_model=None,
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
        delayed_until = compute_delayed_until(delayed_for, delayed_until)

        # create the job or get an existing one
        job_kwargs = {'identifier': identifier, 'queued': '1'}
        retries = 0
        while retries < 10:
            retries += 1
            try:
                job, created = cls.get_or_connect(**job_kwargs)
            except IndexError:
                # Failure during the retrieval https://friendpaste.com/5U63a8aFuV44SEgQckgMP
                # => retry
                continue
            except ValueError:
                # more than one already in the queue !
                try:
                    job = cls.collection(**job_kwargs).instances()[0]
                except IndexError:
                    # but no more now ?!
                    # => retry
                    continue
                else:
                    created = False

            # ok we have our job, stop now
            break

        try:
            # check queue_name
            queue_name = cls._get_queue_name(queue_name)

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
                current_queue.waiting.lrem(0, job.ident)

            else:
                job.set_fields(added=str(datetime.utcnow()), **(fields_if_new or {}))

            # add the job to the queue
            job.enqueue_or_delay(queue_name, priority, delayed_until, prepend, queue_model)

            return job
        except Exception:
            job.queued.delete()
            raise

    def run(self, queue):
        """
        The method called by the work to do some stuff. Must be overriden to
        your needs.
        The optional return value of this function will be passed to the
        job_success method of the worker.
        """
        raise NotImplementedError('You must implement your own action')

    @property
    def must_be_cancelled_on_error(self):
        """
        Return True if either the "always_cancel_on_error" attribute is True,
        of if the "cancel_on_error" field holds a True value
        """
        return self.always_cancel_on_error or self.cancel_on_error.hget()

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

    def requeue(self, queue_name=None, priority=None, delayed_for=None,
                                        delayed_until=None, queue_model=None):
        """
        Requeue the job in the given queue if it has previously failed
        """
        queue_name = self._get_queue_name(queue_name)

        # we can only requeue a job that raised an error
        if self.status.hget() != STATUSES.ERROR:
            raise LimpydJobsException('Job cannot be requeued if not in ERROR status')

        self.hdel('start', 'end')

        if priority is None:
            priority = self.priority.hget()

        delayed_until = compute_delayed_until(delayed_for, delayed_until)

        self.enqueue_or_delay(queue_name, priority, delayed_until, queue_model=queue_model)

    def enqueue_or_delay(self, queue_name=None, priority=None,
                         delayed_until=None, prepend=False, queue_model=None):
        """
        Will enqueue or delay the job depending of the delayed_until.
        """
        queue_name = self._get_queue_name(queue_name)

        fields = {'queued': '1'}

        if priority is not None:
            fields['priority'] = priority
        else:
            priority = self.priority.hget()

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
            queue.delay_job(self, delayed_until)
        else:
            queue.enqueue_job(self, prepend)

    # Methods below will be called only if defined, so we don't create them, but
    # feel free to do so if you need to interact on jobs updates

    # def on_started(self, queue):
    #     pass

    # def on_success(self, queue, result):
    #     pass

    # def on_error(self, queue, exception, trace):
    #     pass

    # def on_skipped(self, queue):
    #     pass

    # def on_requeued(self, queue):
    #     pass


class Error(BaseJobsModel):
    job_model_repr = fields.InstanceHashField(indexable=True)
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
            job_model_repr=job.get_model_repr(),
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

    @classmethod
    def collection_for_job(cls, job):
        """
        Helper to return a collection of errors for the given job
        """
        return cls.collection(job_model_repr=job.get_model_repr(), identifier=getattr(job, '_cached_identifier', job.identifier.hget()))

    @property
    def datetime(self):
        """
        Property which return a real datetime object based on the date and time
        fields
        """
        date, time = self.hmget('date', 'time')
        return parse('%s %s' % (date, time))
