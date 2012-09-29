from dateutil.parser import parse

from limpyd import fields
from limpyd.contrib import database, collection
from limpyd_extensions import related

from limpyd_jobs import STATUSES

__all__ = ('BaseJobsModel', 'Queue', 'Job')


class BaseJobsModel(related.RelatedModel):
    collection_mananger_class = collection.ExtendedCollectionManager
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
    name = fields.HashableField(indexable=True)
    priority = fields.HashableField(indexable=True, default=0)  # the higher, the better
    waiting = fields.ListField()
    success = fields.ListField()
    errors = fields.ListField()

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

    @classmethod
    def get_keys(cls, name):
        """
        Return a list of all queue keys, to use with blpop
        """
        collection = cls.collection(name=name).sort(by='-priority')
        return [col.waiting.key for col in collection.instances()]


class Job(BaseJobsModel):
    identifier = fields.HashableField(indexable=True)  # ex: "myobj:123:update"
    status = fields.HashableField(indexable=True)  # see statuses constants
    priority = fields.HashableField(indexable=True, default=0)
    start = fields.HashableField()
    end = fields.HashableField()

    @classmethod
    def add_job(cls, identifier, queue_name, priority=0, **fields_if_new):
        """
        Add a job to a queue.
        If this job already exists, check it's current priority. If its higher
        than the new one, don't touch it, else move the job to the wanted queue.
        If the job is created, fields in fields_if_new will be set for the new
        job.
        Finally return the job.
        """

        # the queue where we want to add the job
        queue = Queue.get_queue(queue_name, priority)

        # create the job or get an existing one
        job, created = cls.get_or_connect(identifier=identifier, status=STATUSES.WAITING)

        # if the job already exists, and we want a higher priority, update it
        if not created:
            current_priority = int(job.priority.hget() or 0)
            # if the job has a higher priority, don't move it
            if current_priority >= priority:
                return job

            # cancel it temporarily, we'll set is as waiting later
            job.status.hset(STATUSES.CANCELED)

            # remove it from the current queue, we'll add it to the new one later
            current_queue = Queue.get_queue(queue_name, current_priority)
            current_queue.waiting.lrem(0, job.get_pk())

        elif fields_if_new:
            job.set_fields(**fields_if_new)

        # add the job to the new queue with a waiting status

        # set it's status and priority
        job.hmset(status=STATUSES.WAITING, priority=priority)

        # and add it to the new queue
        queue.waiting.rpush(job.get_pk())

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
            raise
            return None
