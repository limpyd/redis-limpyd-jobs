redis-limpyd-jobs
=================

A queue/jobs system based on [redis-limpyd][] ([redis][] orm (sort of) in python)

Github repository: <https://github.com/twidi/redis-limpyd-jobs>

Note that you actually need the [develop branch of the twidi's fork of redis-limpyd][twidi-limpyd], and [redis-limpyd-extensions][]

## How it works

`redis-limpyd-jobs` provides three `limpyd` models (`Queue`, `Job`, `Error`), and a `Worker` class.

These models implement the minimum stuff you need to run jobs asynchronously:

- Use the `Job` model to store things to do
- The `Queue` model will store a list of jobs, with a priority system
- The `Error` model will store all errors
- The `Worker` class is to be used in a process to go through a queue and run jobs

## Simple example

```python
    from limpyd_jobs import STATUSES, Job, Worker

    # The function to run when a job is called by the worker
    def do_stuff(job, queue):
        # here do stuff with your job

    # Create a first job, name 'job:1', in a queue named 'myqueue', with a
    # priority of 1. The higher the priority, the sooner the job will run
    job1 = Job.add_job(identifier='job:1', queue_name='myqueue', priority=1)

    # Add another job in the same queue, with a higher priority, and a different
    # identifier (if the same was used, no new job would be added, but the
    # existing job's priority would have been updated)
    job2 = Job.add_job(identifier='job:2', queue_name='myqueue', priority=2)

    # Create a worker for the queue used previously, asking to call the 
    # "do_stuff" function for each job, and to stop after 2 jobs
    worker = Worker(name='myqueue', callback=do_stuff, max_loops=2)

    # Now really run the job
    worker.run()

    # Here our jobs are done, our queue is empty
    queue = Queue.get_queue('myqueue')
    # nothing waiting
    print queue.waiting.lmembers()
    >> []
    # two jobs in success
    print queue.success.lmembers()
    >> ['jobs:job:1', 'jobs:job:2']

    # Check our jobs statuses
    print job1.status.hget() == STATUSES.SUCCESS
    >> True
    print job2.status.hget() == STATUSES.SUCCESS
    >> True
```

You notice how it works:

- `Job.add_job` to create a job
- `Worker()` to create a worker, with `callback` argument to set which function to run for each job
- `worker.run` to launch a worker.

Notice that you can run as much workers as you want, even on the same queue name. Internally, we use the `blpop` redis command to get jobs atomically.
But you can also run only one worker, having only one queue, doing different stuff in the callback depending on the `idenfitier` attribute of the job.

If you want to store more information in a job, queue ou error, or want to have a different behavior in a worker, it's easy because you can create subclasses of everything in `limpyd-jobs`, the `limpyd` models or the `Worker` class.

## Models

### Job

A Job store all needed information about a task to run. 

#### Job fields

By default it contains a few fields:

##### `identifier`

A string (`HashableField`) to identify the job. 

You can't have many jobs with the same identifier in a waiting queue. If you create a new job with an identifier while an other with the same is still in the same waiting queue, what is done depends on the priority of the two jobs:
- if the new job has a lower (or equal) priority, it's discarded
- if the new job has a higher priority, the priority of the existing job is updated to the higher.
In both cases the `add_job` class method returns the existing job, discarding the new one.

A common way of using the identifier is to, at least, store a way to identify the object on which we want the task to apply:
- you can have one or more queue for a unique task, and store only the `id` of an object on the `identifier` field
- you can have one or more queue each doing many tasks, then you may want to store the task too in the `identifier` "task:id"

Note that by subclassing the `Job` model, you are able to add new fields to a Job to store the task and other needed parameters.

##### `status`

A string (`HashableField`) to store the actual status of the job. 

It's a single letter but we provide a class to help using it verbosely: `STATUSES`

```python
    from limpyd_jobs import STATUSES
    print STATUSES.SUCCESS
    >> "s"
```

When a job is created via the `add_job` class method, its status is set to `STATUSES.WAITING`. When it selected by the worker to execute it, the status passes to `STATUSES.RUNNING`. When finished, it's one of `STATUSES.SUCCESS` or `STATUSES.ERROR`. An other available status is `STATUSES.CANCELED`, useful if you want to cancel a job without removing it from its queue.

##### `priority`

A string (`HashableField`) to store the priority of the job.

The priority of a job determines in which Queue object it will be stored. And a worker listen for all queues with a given name and different priorities, but respecting the priority (reverse) order: the higher the priority, the sooner the job will be executed.

Directly updating the priority of a job will not change the queue in which it's stored. But when you add a job via the (recommended) `add_job` class method, if a job with the same identifier exists, its priority will be updated (only if the new one is higher) and the job will be moved to the higher priority queue.

##### `start`

A string (`HashableField`) to store the date and time (a string representation of `datetime.utcnow()`) of the moment the job was fetched from the queue, just before the callback is called.

It's useful in combination of the `end` field to calculate the job duration.

##### `end`

A string (`HashableField`) to store the date and time (a string representation of `datetime.utcnow()`) of the moment the job was set as finished or in error, just after the has finished.

It's useful in combination of the `start` field to calculate the job duration.

#### Job properties and methods

The `Job` model contains only one property, and no methods:

##### `duration` (property)

The `duration` property simple returns the time used to compute the job. The return value is a `datetime.timedelta` object if the `start` and `end` fields are set, or `None` on the other case.

#### Job class methods

The `job` model provides a single, but very important, class method:

##### `add_job`

The `add_job` class method is the main (and recommended) way to create a job. It will check if a job with the same identifier already exists in a waiting queue and if one is found, update its priority (and move it in the correct queue). And if no existing job is found, a new one will be created and added to a queue.

Arguments:

- `identifier`
    The value for the `identifier` field

- `queue_name`
    The queue name in which to save the job

- `priority=0`
    The priority of the new job, or the new priority of an already existing job, if this priority is higher of the existing one.
    

If you use a subclass of the `Job` model, you can pass additional arguments to the `add_job` method simply by passing them as named arguments, they will be save if a new job is created (but not if an existing job is found in a waiting queue)

### Queue

(documentation is coming...)

### Error

(documentation is coming...)

## The worker(s)

### Worker class

(documentation is coming...)


## The end.

[redis-limpyd-extensions]: https://github.com/twidi/redis-limpyd-extensions
[redis]: http://redis.io
[redis-limpyd]: https://github.com/yohanboniface/redis-limpyd
[twidi-limpyd]: https://github.com/twidi/redis-limpyd/tree/develop
