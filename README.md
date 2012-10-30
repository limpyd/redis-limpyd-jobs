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

And workers are able to catch SIGINT/SIGTERM signals, finishing executing the current job before exiting. Useful if used, for example, with supervisord.

If you want to store more information in a job, queue ou error, or want to have a different behavior in a worker, it's easy because you can create subclasses of everything in `limpyd-jobs`, the `limpyd` models or the `Worker` class.

## Models

### Job

A Job store all needed information about a task to run. 

#### Job fields

By default it contains a few fields:

##### `identifier`

A string (`HashableField`, indexed) to identify the job. 

You can't have many jobs with the same identifier in a waiting queue. If you create a new job with an identifier while an other with the same is still in the same waiting queue, what is done depends on the priority of the two jobs:
- if the new job has a lower (or equal) priority, it's discarded
- if the new job has a higher priority, the priority of the existing job is updated to the higher.

In both cases the `add_job` class method returns the existing job, discarding the new one.

A common way of using the identifier is to, at least, store a way to identify the object on which we want the task to apply:
- you can have one or more queue for a unique task, and store only the `id` of an object on the `identifier` field
- you can have one or more queue each doing many tasks, then you may want to store the task too in the `identifier` "task:id"

Note that by subclassing the `Job` model, you are able to add new fields to a Job to store the task and other needed parameters.

##### `status`

A string (`HashableField`, indexed) to store the actual status of the job. 

It's a single letter but we provide a class to help using it verbosely: `STATUSES`

```python
    from limpyd_jobs import STATUSES
    print STATUSES.SUCCESS
    >> "s"
```

When a job is created via the `add_job` class method, its status is set to `STATUSES.WAITING`. When it selected by the worker to execute it, the status passes to `STATUSES.RUNNING`. When finished, it's one of `STATUSES.SUCCESS` or `STATUSES.ERROR`. An other available status is `STATUSES.CANCELED`, useful if you want to cancel a job without removing it from its queue.

You can also display the full string of a status:

```python
    print STATUSES.by_value(my_job.status.hget())
```

##### `priority`

A string (`HashableField`, indexed, default = 0) to store the priority of the job.

The priority of a job determines in which Queue object it will be stored. And a worker listen for all queues with a given name and different priorities, but respecting the priority (reverse) order: the higher the priority, the sooner the job will be executed.

Directly updating the priority of a job will not change the queue in which it's stored. But when you add a job via the (recommended) `add_job` class method, if a job with the same identifier exists, its priority will be updated (only if the new one is higher) and the job will be moved to the higher priority queue.

##### `start`

A string (`HashableField`) to store the date and time (a string representation of `datetime.utcnow()`) of the moment the job was fetched from the queue, just before the callback is called.

It's useful in combination of the `end` field to calculate the job duration.

##### `end`

A string (`HashableField`) to store the date and time (a string representation of `datetime.utcnow()`) of the moment the job was set as finished or in error, just after the has finished.

It's useful in combination of the `start` field to calculate the job duration.

#### Job attributes

There is only one attribute on the `Job` model, but it is very important:

##### `queue_model`

When adding jobs via the `add_job` method, the model defined in this attribute will be used to get or create a queue. It's set by default to `Queue` but if you want to update it to your own model, you must subclass the `Job` model too, and update this attribute.

Note that if you don't subclass the `Job` model, you can pass the `queue_model` argument to the `add_job` method.

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

- `queue_model`
    The model to use to store queues. By default, it's set to `Queue`, defined in the `queue_model` attribute of the `Job` model. If the argument is not set, the attribute will be used. Be careful to set it as attribute in your subclass, or as argument in `add_job` or the default `Queue` model will be used and jobs won't be saved in the expected queue.

- `prepend=False`
    If by default, all new jobs are added at the end of the waiting list (and taken from the start, it's a fifo list), but you can force jobs to be added at the beginning of the waiting list to be the first to be executed, simply by setting the `prepend` argument to True. If the job already exists, it will be moved at the beginning of the list.
    

If you use a subclass of the `Job` model, you can pass additional arguments to the `add_job` method simply by passing them as named arguments, they will be save if a new job is created (but not if an existing job is found in a waiting queue)

### Queue

A Queue store a list of waiting jobs with a given priority, and keep a list of successful jobs and ones on error.

#### Queue fields

By default it contains a few fields:

##### `name`

A string (`HashableField`, indexed), used by the `add_job` method to find the queue in which to store it. Many queues can have the same name, but different priorities.

This name is also used by a worker to find which queues it needs to wait for.

##### `priority`

A string (`HashableField`, indexed, default = 0), to store the priority of a queue's jobs. All jobs in a queue are considered having this priority. It's why, as said for the `property` fields of the `Job` model, changing the property of a job doesn't change its real property. But adding a new job with the same identifier for the same queue's name can update the job's priority by moving it to another queue with the correct priority.

As already said, the higher the priority, the sooner the jobs in a queue will be executed. If a queue has a priority of 2, and another queue of the same name has a priority of 0, or 1, *all* jobs in the one with the priority of 2 will be executed (at least fetched) before the others, (quel que soit) the number of workers.

##### `waiting`

A list (`ListField`) to store the primary keys of job in the waiting status. It's a fifo list: jobs are appended to the right (via `rpush`, and fetched from the left (via `blpop`)

When fetched, a job from this list is executed, then pushed in the `error` or `success` list, depending if the callback raised an exception.
If a job in this waiting list is not in the waiting status, it will be skipped by the worker.

##### `success`

A list (`ListField`) to store the primary keys of jobs fetched from the waiting list and successfully executed. 

##### `error`

A list (`ListField`) to store the primary keys of jobs fetched from the waiting list for which the execution failed.

#### Queue attributes

The `Queue` model has no specific attribute.

#### Queue properties and methods

The `Queue` model has no specific properties or method.

#### Queue class methods

The `Queue` model provides two class methods:

##### `get_queue`

The `get_queue` class method is the recommended way to get a `Queue` object. Given a name and a priority, it will return the found queue or create a queue if no matching one exist.

Arguments:

- name
    The name of the queue to get or create

- priority
    The priority of the queue to get or create

If you use a subclass of the `Queue` model, you can pass additional arguments to the `get_queue` method simply by passing them as named arguments, they will be save if a new queue is created (but not if an existing queue is found)

##### `get_keys`

The `get_keys` class method returns all the existing queue with a given name, sorted by priority (reverse order: the highest priorities come first).
The returned value is a list of redis keys for each `waiting` lists of matching queues. It's used internally by the workers as argument to the `blpop` redis command.

### Error

The `Error` model is used to store errors from the job that are not successfully executed by a worker.

Its main purpose is to be able to filter errors, by queue name, job identifier, date, exception class name or code. You can use your own subclass of the `Error` model and then store additional fields, and filter on them.

#### Error fields

##### `idenfitier`

A string (`HashableField`, indexed) to store the identifier of the job that failed.

##### `queue_name`

A string (`HashableField`, indexed) to store the name of the queue the job was in when it failed.

##### `date`

A string (`HashableField`, indexed) to store the date (only the date, not the time) of the error (a string representation of `datetime.utcnow().date()`). This field is indexed so you can filter errors by date, useful to graph errors.

##### `time`

A string (`HashableField`) to store the time (only the time, not the date) of the error (a string representation of `datetime.utcnow().time()`).

##### `type`

A string (`HashableField`, indexed) to store the type of error. It's the class' name of the originally raised exception.

##### `code`

A string (`HashableField`, indexed) to store the value of the `code` attribute of the originally raised exception. Nothing is stored here if there is no such attribute.

##### `message`

A string (`HashableField`) to store the string representation of the originally raised exception.

#### Error properties and methods

There is only one property on the `Error` model:

##### `datetime`

This property returns a `datetime` object based on the content of the `date` and `time` fields of an `Error` object.

#### Error class methods

The `Error` model doesn't provide any class method.

## The worker(s)

### The Worker class

The `Worker` class does all the logic, working with `Queue` and `Job` models.

The main behavior is:
- reading queue keys for the given name
- waiting for a job available in the queue
- executing the job
- manage success or error
- exit after a defined number of jobs

The class is split in many short methods so that you can subclass it to change/add/remove whatever you want.

#### Constructor arguments and worker's attributes

Each of the following worker's attributes can be set by an argument in the constructor, using the exact same name. It's why the two are described here together.

##### `name`

The name of the worker, used to get all queues with this name. Default to `None`, but if not set, will raise an `ImplementationError`

##### `job_model`

The model to use for jobs. By default it's the `Job` model included in `limpyd_jobs`, but you can use a subclass of the default model to add fields, methods...

##### `queue_model`

The model to use for queues. By default it's the `Queue` model included in `limpyd_jobs`, but you can use a subclass of the default model to add fields, methods...

##### `error_model`

The model to use for saving errors. By default it's the `Error` model included in `limpyd_jobs`, but you can use a subclass of the default model to add fields, methods...

##### `logger_base_name`

`limpyd_jobs` uses the python `logging` module, so this is the name to use for the logger created for the worker. The default value is `LOGGER_BASE_NAME + '.%s'`, with `LOGGER_BASE_NAME` defined in `limpyd_jobs.workers` with a value of "limpyd_jobs", and '%s' will be replaced by the `name` attribute.

##### `logger_level`

It's the level set for the logger created with the name defined in `logger_base_name`.

##### `save_errors`

A boolean, default to True, to indicate if we have to save errors in the `Error` model (or the one defined in `error_model`) when the execution of the job is not successful.

##### `max_loops`

The max number of loops (fetching + executing a job) to do in the worker lifetime, default to 1000. Note that after this number of loop, the worker ends (the `run` method cannot be executed again.)

##### `terminate_gracefully`

To avoid interrupting the execution of a job, if `terminate_gracefully` is set to True (the default), the `SIGINT` and `SIGTERM` signals are caught, asking the worker to exit when the current jog is done.

##### `callback`

The callback is the function to run when a job is fetched. By default it's the `execute` method of the worker (which, if not overridden, raises a `NotImplemented` error) , but you can pass any function that accept a job and a queue as argument.

If this callback (or the `execute` method) raises an exception, the job is considered in error. In the other case, it's considered successful and the return value is passed to the `job_success` method, to let you do what you want with it.

##### `timeout`

The timeout is used as parameter to the `blpop` redis command we use to fetch jobs from waiting lists. It's 30 seconds by default but you can change it any positive numbers (in seconds). You can set it to `0` if you don't want any timeout be applied to the `blpop` command. 

It's better to always set a timeout, to reenter the main loop and call the `must_stop` method to see if the worker must exit.
Note that the number of loops is not updated in this case, so a little `timeout` won't alter the number of loops defined by `max_loops`.

#### Other worker's attributes

In case on subclassing, you can need these attributes, created and defined during the use of the worker:

##### `keys`

A list of keys of queues waiting lists, which are listened by the worker for new jobs. Filled by the `update_keys` method.

##### `status`

The current status of the worker. `None` by default until the `run` method is called. Then it's set to `"waiting"` while the worker waits for new jobs. When a job is fetched, the status is set to `"running"`. And finally, when the loop is over, it's set to `"terminated"`. 

If the status is not `None`, the `run` method cannot be called.

##### `logger`

The logger (from the `logging` python module) defined by the `set_logger` method.

##### `num_loops`

The number of loops done by the worker, incremented each time a job is fetched from a waiting list, even if the job is skipped (bad status...), or in error.
When this number equals the `max_loops` attribute, the worker ends.

##### `end_forced`

When `True`, ask for the worker to terminate itself after executing the current job. It can be set to `True` manually, or when:
- there is no key to wait for in redis
- a SIGINT/SIGTERM signal is caught

##### `end_signal_caught`

This boolean is set to `True` when a SIGINT/SIGTERM is caught (only if the `terminate_gracefully` is `True`)

##### `connection`

It's a property, not an attribute, to get the current connection to the redis server.

#### Worker's methods

As said before, the `Worker` class in spit in many little methods, to ease subclassing. Here is the list of public methods:

##### `__init__`

Signature:     

```python
    def __init__(self, name=None, callback=None,
                 queue_model=None, job_model=None, error_model=None,
                 logger_base_name=None, logger_level=None, save_errors=None,
                 max_loops=None, terminate_gracefuly=None):
```
Returns nothing.

It's the constructor (you know that ;) ) of the `Worker` class, excepting all arguments that can also be defined as class attributes.

It validates these arguments, prepares the logging and initializes other attributes.

You can override it to add, validate, initialize other arguments or attributes.

##### `handle_end_signal`

Signature:

```python
    def handle_end_signal(self)
```
Returns nothing.

It's called in the constructor if `terminate_gracefully` is True. It plugs the SIGINT and SIGTERM signal to the `catch_end_signal` method.

You can override it to catch more signals or do some checked before plugging them to the `catch_end_signal` method.

##### `set_logger`

Signature:

```python
def set_logger(self)
```
Returns nothing.

It's called in the constructor to initialize the logger, using `logger_base_name` and `logger_level`, saving it in `self.logger`.

##### `must_stop`

Signature:

```python
def must_stop(self)
```
Returns boolean.

It's called on the main loop, to exit it on some conditions: a end signal was caught, the max_loops number was reached, or `end_forced` was set to True.

##### `wait_for_job`

Signature:

```python
def wait_for_job(self)
```
Returns a tuple with a Queue and a Job

This method is called during the loop, to wait for available job in the waiting lists. When one job is fetched, returns the queue (an instance of the model defined by `queue_model`) on which the job was found, and the job itself (an instance of the model defined by `job_model`).

##### `get_job`

Signature:

```python
def get_job(self, job_pk)
```
Returns a Job

Called during `wait_for_job` to get a real job object based on the primary key fetched from the waiting lists.

##### `get_queue`

Signature:

```python
def get_queue(self, queue_redis_key)
```
Returns a Queue

Called during `wait_for_job` to get a real queue object based on the key returned by redis telling us in which list the job was found. This key is not the primary key of the queue, but the redis key of it's waiting field.

##### `catch_end_signal`

Signature:

```python
def catch_end_signal(self, signum, frame)
```
Returns nothing

It's called when a SIGINT/SIGTERM signal is caught. It's simply set `end_signal_caught` and `end_forced` to True, to tell the worker to terminate as soon as possible.

##### `execute`

Signature:

```python
def execute(self, job, queue)
```
Returns nothing by default

This method is called if no `callback` argument is provided when initiating the worker. But raises a `NotImplementedError` by default. To use it (without passing the `callback` argument), you must override it in your own subclass.

If the execution is successful, no return value is attended, but if any, it will be passed to the `job_success` method. And if a error occurred, an exception must be raised, which will be passed to the `job_error` method.

##### `update_keys`

Signature:

```python
def update_keys(self)
```
Returns nothing

Calling this method updates the internal `keys` attributes, which contains redis keys of the waiting lists of all queues listened by the worker (the ones with the same name).

It's actually called at the beginning of the `run` method.
Note that if a queue with a specific priority doesn't exist when this method is called, but later, by adding a job with `add_job`, the worker will ignore it unless this `update_keys` method was called again.

##### `run`

Signature:

```python
def run(self)
```
Returns nothing

It's the main method of the worker, with all the logic: while we don't have to stop, fetch a job from redis, and if this job is really in waiting state, execute it, and to something depending of the status of the execution (success, error...).

In addition to the methods that do real stuff (`update_keys`, `wait_for_job`), some other methods are called during the execution: `run_started`, `run_ended`, about the run, and `job_skipped`, `job_started`, `job_success` and `job_error` about jobs. You can override these methods in subclasses to adapt the behavior depending on your needs.

##### `run_started`

Signature:

```python
def run_started(self)
```
Returns nothing

This method is called in the `run` method after the keys are computed using `update_keys`, just before starting the loop. By default it does nothing but a log.info.

##### `run_ended`

Signature:

```python
def run_ended(self)
```
Returns nothing

This method is called just before exiting the `run` method. By default it does nothing but a log.info.

##### `job_skipped`

Signature:

```python
def job_skipped(self, job, queue, message=None)
```
Returns nothing

When a job is fetched in the `run` method, its status is fetched. If this status is not `STATUSES.WAITING`, the `job_skipped` method is called, with two main arguments: the job and the queue in which it was found.

The `message` argument is here only to help you override the method to only change the message computed, letting the default method doing its internal stuff which in fact is nothing else for `job_skipped` than logging this message.

##### `job_started`

Signature:

```python
def job_started(self, job, queue, message=None)
```
Returns nothing

When the job is fetched and its status verified (it must be `STATUSES.WAITING`), the `job_started` method is called, just before the callback (or the `execute` method if no callback is defined), with the job and the queue in which it was found.

The `message` argument is here only to help you override the method to only change the message computed, letting the default method doing its internal stuff which is simply updating the `start` and `status` fields of the job, then logging the message.

##### `job_success`

Signature:

```python
def job_success(self, job, queue, job_result, message=None)

```
Returns nothing

When the callback (or the `execute` method) is finished, without having raised any exception, the job is considered successful, and the `job_success` method is called, with the job and the queue in which it was found, and the return value of the callback method.

The `message` argument is here only to help you override the method to only change the message computed, letting the default method doing its internal stuff which is updating the `start` and `status` fields of the job, moving the job into the `success` list of the queue and then logging the message.

##### `job_error`

Signature:
```python
def job_error(self, job, queue, exception, message=None)
```
Returns nothing

When the callback (or the `execute` method) is terminated by raising an exception, and the `job_error` method is called, with the job and the queue in which it was found, and the raised exception.

The `message` argument is here only to help you override the method to only change the message computed, letting the default method doing its internal stuff which is updating the `start` and `status` fields of the job, moving the job into the `error` list of the queue, adding a new error object (if `save_errors` is True) and then logging the message.

##### `additional_error_fields`

Signature:
```python
def additional_error_fields(self, job, queue, exception)
```
Returns a dictionary of fields to add to the error object.

This method is called by `job_error` to let you define a dictionary of fields/values to add to the error object which will be created, if you use a subclass of the `Error` model, defined in `error_model`.

##### `id`

It's a property returning a string identifying the current work, used in logging to distinct log entries for each worker.

##### `log`

Signature:
```python
def log(self, message, level='info')
```
Returns nothing

`log` is a simple wrapper around `self.logger`, which automatically add the `id` of the worker at the beginning. It can accepts a `level` argument which is `info` by default.

## Final words

- you can see a full example in `example.py`
- to use `limpyd_jobs` models on your own redis database instead of the default one (`localhost:6379:db=0`), simply use the `use_database` method of the main model:

    ```python
    from limpyd.contrib.database import PipelineDatabase
    from limpyd_jobs.models import BaseJobsModel

    database = PipelineDatabase(host='localhost', port=6379, db=15)
    BaseJobsModel.use_database(database)
    ```

    or simply change the connection settings:

    ```python
    from limpyd_jobs.models import BaseJobsModel

    BaseJobsModel.database.connect(host='localhost', port=6379, db=15)
    ```


## The end.

[redis-limpyd-extensions]: https://github.com/twidi/redis-limpyd-extensions
[redis]: http://redis.io
[redis-limpyd]: https://github.com/yohanboniface/redis-limpyd
[twidi-limpyd]: https://github.com/twidi/redis-limpyd/tree/develop
