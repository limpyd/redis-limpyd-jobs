"""
This is a complex example. Forget the fact that we launch workers in threads,
it's only for the example: you may launch them in separate processes with a
supervisor (like supervisord), using, or not, scripts/limpyd-worker.py
Here, we subclass the Queue and Job model, to add new fields, and the Worker
class to do specific stuff.
The purpose of this example is to show you how you can use the possibilities
offered by the limpyd_jobs models/classes to do advanced things.
"""
from datetime import datetime
import time
import random
import threading
import logging

from limpyd_jobs import STATUSES
from limpyd_jobs.models import BaseJobsModel, Queue, Job
from limpyd_jobs.workers import Worker, logger
from limpyd import model, fields

# start by defining our logger
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# we have to plug our models on a database
BaseJobsModel.database.connect(host='localhost', port=6379, db=15)

# we'll use only one queue name
QUEUE_NAME = 'update_fullname'


class MyQueue(Queue):
    """
    A queue that will store the dates of it's first and last successful job
    """
    first_job_date = fields.InstanceHashField()
    last_job_date = fields.InstanceHashField()
    jobs_counter = fields.InstanceHashField()


class MyJob(Job):
    """
    A job that will use Person's PK as identifier, and will store results of
    callback in a new field
    """
    result = fields.StringField()  # to store the result of the task
    queue_model = MyQueue
    queue_name = QUEUE_NAME

    start = fields.InstanceHashField(indexable=True)

    def get_object(self):
        """
        Return the object concerned by the current job's identifier
        """
        return Person.get(self.identifier.hget())

    def run(self, queue):
        """
        Create the fullname, and store a a message serving as result in the job
        """
        # add some random time to simulate a long job
        time.sleep(random.random())

        # compute the fullname
        obj = self.get_object()
        obj.fullname.hset('%s %s' % tuple(obj.hmget('firstname', 'lastname')))

        # this will the "result" of the job
        result = 'Created fullname for Person %s: %s' % (obj.pk.get(), obj.fullname.hget())

        # save the result of the callback in the job itself
        self.result.set(result)

        # return the result for future use in the worker
        return result


class Person(model.RedisModel):
    """
    A simple model for which we want to compute fullname based on firstname and
    lastname
    """
    database = BaseJobsModel.database

    firstname = fields.InstanceHashField()
    lastname = fields.InstanceHashField()
    fullname = fields.InstanceHashField()


class FullNameWorker(Worker):
    """
    A worker class to update fullname field of Person
    """

    # we work on a specific queue
    queues = [QUEUE_NAME]

    # we use our own models
    queue_model = MyQueue
    job_model = MyJob

    # useful logging level
    logger_level = logging.INFO

    # reduce timeout and number of loops
    max_loops = 6
    timeout = 5

    # workers will be created in threads for this example, and signals only
    # works in main thread
    terminate_gracefuly = False

    # Force stop after at least 5 seconds
    max_duration = 5

    def __init__(self, *args, **kwargs):
        # keep a list of jobs, to display at the end of this example script
        self.jobs = []
        super(FullNameWorker, self).__init__(*args, **kwargs)

    def job_success(self, job, queue, job_result):
        """
        Update the queue's dates and number of jobs managed, and save into the
        job the result received by the callback.
        """

        # display what was done
        obj = job.get_object()
        message = '[%s|%s] %s [%s]' % (queue.name.hget(),
                                       obj.pk.get(),
                                       job_result,
                                       threading.current_thread().name)
        self.log(message)

        # default stuff: update job and queue statuses, and do logging
        super(FullNameWorker, self).job_success(job, queue, job_result)

        # update the queue's dates
        queue_fields_to_update = {
            'last_job_date': str(datetime.utcnow())
        }
        if not queue.first_job_date.hget():
            queue_fields_to_update['first_job_date'] = queue_fields_to_update['last_job_date']
        queue.hmset(**queue_fields_to_update)

        # update the jobs counter on the queue
        queue.jobs_counter.hincrby(1)

        # save a ref to the job to display at the end of this example script
        self.jobs.append(int(job.pk.get()))


class WorkerThread(threading.Thread):
    """
    A simple thread which will run a worker
    """
    def run(self):
        worker = FullNameWorker()
        workers.append(worker)
        worker.run()


def clean():
    """
    Clean data created by this script
    """
    for queue in MyQueue.collection().instances():
        queue.delete()
    for job in MyJob.collection().instances():
        job.delete()
    for person in Person.collection().instances():
        person.delete()
clean()

# create some persons
for name in ("Chandler Bing", "Rachel Green", "Ross Geller", "Joey Tribbiani",
             "Phoebe Buffay", "Monica Geller", "Carol Willick", "Barry Farber",
             "Janice Goralnick", "Ursula Buffay", "Nora Bing", "Richard Burke",
             "Estelle Leonard", "Pete Becker", "Paul Stevens",
             "Mindy Hunter-Farber", "Ben Geller", "Jack Geller", "Judy Geller",
             "Frank Buffay", "Mark Robinson", "Emily Waltham"):
    firstname, lastname = name.split(' ')
    Person(firstname=firstname, lastname=lastname)


# add jobs
for person_pk in Person.collection():
    MyJob.add_job(identifier=person_pk, priority=random.randrange(5))


# create 3 workers, in threads
threads = []
workers = []
for i in range(3):
    thread = WorkerThread()
    thread.start()
    threads.append(thread)

# wait for threads to finish
for thread in threads:
    thread.join()

# and print some informations about all our data
print '\nALL DONE !'

print '\nPersons:'
for person in Person.collection().instances():
    firstname, lastname, fullname = person.hmget('firstname', 'lastname', 'fullname')
    print '\t[%s] "%s" "%s" => "%s"' % (
        person.pk.get(), firstname, lastname, fullname or '*NOT EXECUTED*'
    )

print '\nJobs:'
for job in MyJob.collection(status=STATUSES.SUCCESS).sort(by='start', alpha=True).instances():
    identifier, priority, result = job.hmget('identifier', 'priority', 'result')
    print '\t[%s] (identifier: %s, priority %s) executed in %s => %s' % (
        job.pk.get(), identifier, priority, job.duration, job.result.get()
    )
for job in MyJob.collection(status=STATUSES.WAITING).sort(by='identifier').instances():
    identifier, priority = job.hmget('identifier', 'priority')
    print '\t[%s] (identifier: %s, priority %s) waiting' % (
        job.pk.get(), identifier, priority
    )

print '\nWorkers:'
for worker in workers:
    print '\t[%s] executed %s jobs: %s' % (worker.id, worker.num_loops, worker.jobs)

print '\nQueues:'
for queue in MyQueue.collection().sort(by='priority').instances():
    name, priority, counter = queue.hmget('name', 'priority', 'jobs_counter')
    success_part = waiting_part = ''
    if counter:
        success_part = ' Executed %s jobs: %s' % (counter, queue.success.lmembers())
    if queue.waiting.llen():
        waiting_part = ' Still waiting: %s' % queue.waiting.lmembers()
    print '\t[%s] (priority: %s).%s%s' % (name, priority, success_part, waiting_part)

# final clean
clean()
