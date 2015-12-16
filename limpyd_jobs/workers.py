from __future__ import print_function
from __future__ import unicode_literals
from future.builtins import str
from future.builtins import object
from past.builtins import basestring

import logging
import signal
import sys
import threading
import traceback
import os.path

from datetime import datetime, timedelta
from optparse import make_option, OptionParser
from time import sleep

from dateutil.parser import parse
from setproctitle import setproctitle

from limpyd import __version__ as limpyd_version
from limpyd.exceptions import DoesNotExist

from limpyd_jobs.version import __version__ as limpyd_jobs_version
from limpyd_jobs import STATUSES, LimpydJobsException, ConfigurationException
from limpyd_jobs.models import Queue, Job, Error
from limpyd_jobs.utils import import_class, total_seconds, compute_delayed_until

LOGGER_NAME = 'limpyd-jobs'
logger = logging.getLogger(LOGGER_NAME)


class Worker(object):
    # names of queues to use
    queues = None

    # models to use (for all queues/errors !)
    queue_model = Queue
    error_model = Error

    # logging information
    logger_name = LOGGER_NAME
    logger_level = logging.INFO
    save_errors = True
    save_tracebacks = True

    # maximum number of loops to run
    max_loops = 1000
    # max total duration of the worker
    max_duration = None
    # max delay for blpop
    timeout = 30
    # minimum time in seconds between the update of keys to fetch
    # by default < timeout to be sure to fetch them after a timeout occured
    fetch_priorities_delay = 25
    # minimum time in seconds between the update of delayed jobs
    # by default < timeout to be sure to fetch them after a timeout occured
    fetch_delayed_delay = 25
    # number of time to requeue a job after a failure
    requeue_times = 0
    # delta to add to the current priority of a job to be requeued
    requeue_priority_delta = -1
    # how much time to delay a job to be requeued
    requeue_delay_delta = 30

    # we want to intercept SIGTERM and SIGINT signals, to stop gracefuly
    terminate_gracefuly = True

    # callback to run for each job, will be set to self.execute if None
    callback = None

    # all parameters that can be passed to the constructor
    parameters = ('queues', 'callback', 'queue_model', 'error_model',
                  'logger_name', 'logger_level', 'save_errors',
                  'save_tracebacks', 'max_loops', 'max_duration',
                  'terminate_gracefuly', 'timeout', 'fetch_priorities_delay',
                  'fetch_delayed_delay', 'requeue_times',
                  'requeue_priority_delta', 'requeue_delay_delta')

    @staticmethod
    def _parse_queues(queues):
        """
        Parse the given parameter and return a list of queues.
        The parameter must be a list/tuple of strings, or a string with queues
        separated by a comma.
        """
        if not queues:
            raise ConfigurationException('The queue(s) to use are not defined')

        if isinstance(queues, (list, tuple)):
            for queue_name in queues:
                if not isinstance(queue_name, str):
                    raise ConfigurationException('Queue name "%s" is not a (base)string')

        elif isinstance(queues, basestring):
            queues = str(queues).split(',')

        else:
            raise ConfigurationException('Invalid format for queues names')

        return list(queues)

    def __init__(self, queues=None, **kwargs):
        """
        Create the worker by saving arguments, doing some checks, preparing
        logger and signals management, and getting queues keys.
        """
        if queues is not None:
            self.queues = queues
        self.queues = self._parse_queues(self.queues)

        for parameter in self.parameters:
            if parameter in kwargs:
                setattr(self, parameter, kwargs[parameter])

        # save and check models to use
        self._assert_correct_model(self.queue_model, Queue, 'queue')
        self._assert_correct_model(self.error_model, Error, 'error')

        # process other special arguments
        if not self.callback:
            self.callback = self.execute
        if self.max_duration:
            self.max_duration = timedelta(seconds=self.max_duration)

        # prepare logging
        self.set_logger()

        if self.terminate_gracefuly:
            self.handle_end_signal()

        self.keys = []  # list of redis keys to listen
        self.num_loops = 0  # loops counter
        self.start_date = None  # set when run is called
        self.end_date = None  # set when run ends
        self.wanted_end_date = None  # will be the end after which the worker must stop
        self.end_forced = False  # set it to True in "execute" to force stop just after
        self.status = None  # is set to None/waiting/running by the worker
        self.end_signal_caught = False  # internaly set to True if end signal caught
        self.last_fetch_priorities = None  # last time the keys to fetch were updated
        self.last_requeue_delayed = None  # last time the ready delayed jobs where requeued

        self._update_status_callbacks = []  # callbacks to call when status is updated

    @staticmethod
    def _assert_correct_model(model_to_check, model_reference, obj_name):
        """
        Helper that asserts the model_to_check is the model_reference or one of
        its subclasses. If not, raise an ImplementationError, using "obj_name"
        to describe the name of the argument.
        """
        if not issubclass(model_to_check, model_reference):
            raise ConfigurationException('The %s model must be a subclass of %s'
                                         % (obj_name, model_reference.__name__))

    def handle_end_signal(self):
        """
        Catch some system signals to handle them internaly
        """
        try:
            signal.signal(signal.SIGTERM, self.catch_end_signal)
            signal.signal(signal.SIGINT, self.catch_end_signal)
        except ValueError:
            self.log('Signals cannot be caught in a Thread', level='warning')

    def stop_handling_end_signal(self):
        """
        Stop handling the SIGINT and SIGTERM signals
        """
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
        except ValueError:
            self.log('Signals cannot be caught in a Thread', level='warning')

    def set_logger(self):
        """
        Prepare the logger, using self.logger_name and self.logger_level
        """
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(self.logger_level)

    @property
    def id(self):
        """
        Return an identifier for the worker to use in logging
        """
        if not hasattr(self, '_id'):
            self._id = str(threading.current_thread().ident + id(self))[-6:]
        return self._id

    def log(self, message, level='info'):
        """
        Call self.logger with the given level (default to "info") and message
        """
        getattr(self.logger, level)('[%s] %s' % (self.id, message))

    @property
    def connection(self):
        """
        Return the redis connection to use.
        """
        return self.queue_model.get_connection()

    def must_stop(self):
        """
        Return True if the worker must stop when the current loop is over.
        """
        return bool(self.terminate_gracefuly and self.end_signal_caught
                 or self.num_loops >= self.max_loops or self.end_forced
                 or self.wanted_end_date and datetime.utcnow() >= self.wanted_end_date)

    def set_status(self, status):
        """
        Save the new status and call all defined callbacks
        """
        self.status = status
        for callback in self._update_status_callbacks:
            callback(self)

    def _add_update_status_callback(self, callback):
        """
        Add a callback to call when the status is updated
        """
        self._update_status_callbacks.append(callback)

    def wait_for_job(self):
        """
        Use a redis blocking list call to wait for a job, and return it.
        """
        blpop_result = self.connection.blpop(self.keys, self.timeout)
        if blpop_result is None:
            return None
        queue_redis_key, job_ident = blpop_result
        self.set_status('running')
        return self.get_queue(queue_redis_key), self.get_job(job_ident)

    def get_job(self, job_ident):
        """
        Return a job based on its representation (class + pk).
        """
        return Job.get_from_ident(job_ident)

    def get_queue(self, queue_redis_key):
        """
        Return a queue based on the key used in redis to store the list
        """
        try:
            queue_pk = int(queue_redis_key.split(':')[-2])
        except:
            raise DoesNotExist('Unable to get the queue from the key %s' % queue_redis_key)
        return self.queue_model.get(queue_pk)

    def catch_end_signal(self, signum, frame):
        """
        When a SIGINT/SIGTERM signal is caught, this method is called, asking
        for the worker to terminate as soon as possible.
        """
        if self.end_signal_caught:
            self.log('Previous signal caught, will end soon')
            return

        signal_name = dict((getattr(signal, n), n) for n in dir(signal)
                        if n.startswith('SIG') and '_' not in n).get(signum, signum)

        if self.status == 'running':
            self.log('Catched %s signal: stopping after current job' % signal_name,
                     level='critical')
        else:
            delay = self.timeout if self.status == 'waiting' else self.fetch_priorities_delay
            self.log('Catched %s signal: stopping in max %d seconds' % (
                     signal_name, delay), level='critical')

        self.end_signal_caught = self.end_forced = True

    def execute(self, job, queue):
        """
        The method to run for a job when got bak from the queue if no callback
        was defined on __init__.
        The optional return value of this function will be passed to the
        job_success method.
        """
        return job.run(queue)

    def update_keys(self):
        """
        Update the redis keys to listen for new jobs priorities.
        """
        self.keys = self.queue_model.get_waiting_keys(self.queues)

        if not self.keys:
            self.log('No queues yet', level='warning')
        self.last_update_keys = datetime.utcnow()

    def count_waiting_jobs(self):
        """
        Return the number of all jobs waiting in queues managed by the worker
        """
        return self.queue_model.count_waiting_jobs(self.queues)

    def count_delayed_jobs(self):
        """
        Return the number of all delayed jobs in queues managed by the worker
        """
        return self.queue_model.count_delayed_jobs(self.queues)

    def run_started(self):
        """
        Called just before starting to wait for jobs. Actually only do logging.
        """
        self.log('Run started.')

    def run(self):
        """
        The main method of the worker. Will ask redis for list items via
        blocking calls, get jobs from them, try to execute these jobs, and end
        when needed.
        """
        # if status is not None, we already had a run !
        if self.status:
            self.set_status('aborted')
            raise LimpydJobsException('This worker run is already terminated')

        self.set_status('starting')

        self.start_date = datetime.utcnow()
        if self.max_duration:
            self.wanted_end_date = self.start_date + self.max_duration

        must_stop = self.must_stop()

        if not must_stop:

            # get keys
            while not self.keys and not must_stop:
                self.update_keys()
                if not self.keys:
                    sleep(self.fetch_priorities_delay)
                must_stop = self.must_stop()

            if not must_stop:
                # wait for queues available if no ones are yet
                self.requeue_delayed_jobs()
                self.run_started()
                self._main_loop()

        self.set_status('terminated')
        self.end_date = datetime.utcnow()
        self.run_ended()
        if self.terminate_gracefuly:
            self.stop_handling_end_signal()

    @property
    def elapsed(self):
        """
        Return a timedelta representation of the time passed sine the worker
        was running.
        """
        if not self.start_date:
            return None
        return (self.end_date or datetime.utcnow()) - self.start_date

    def requeue_delayed_jobs(self):
        """
        Requeue each delayed job that are now ready to be executed
        """
        failures = []
        for queue in self.queue_model.get_all_by_priority(self.queues):
            failures.extend(queue.requeue_delayed_jobs())

        self.last_requeue_delayed = datetime.utcnow()

        for failure in failures:
            self.log('Unable to requeue %s: %s' % failure)

    def _main_loop(self):
        """
        Run jobs until must_stop returns True
        """
        fetch_priorities_delay = timedelta(seconds=self.fetch_priorities_delay)
        fetch_delayed_delay = timedelta(seconds=self.fetch_delayed_delay)

        while not self.must_stop():
            self.set_status('waiting')

            if self.last_update_keys + fetch_priorities_delay < datetime.utcnow():
                self.update_keys()

            if self.last_requeue_delayed + fetch_delayed_delay < datetime.utcnow():
                self.requeue_delayed_jobs()

            try:
                queue_and_job = self.wait_for_job()
                if queue_and_job is None:
                    # timeout for blpop
                    continue
                queue, job = queue_and_job
            except Exception as e:
                self.log('Unable to get job: %s\n%s'
                    % (str(e), traceback.format_exc()), level='error')
            else:
                self.num_loops += 1
                try:
                    identifier = 'pk:%s' % job.pk.get()
                except Exception as e:
                    identifier = '??'
                try:
                    self.set_status('running')
                    identifier, status = job.hmget('identifier', 'status')
                     # some cache, don't count on it on subclasses
                    job._cached_identifier = identifier
                    job._cached_status = status
                    queue._cached_name = queue.name.hget()

                    if status == STATUSES.DELAYED:
                        self.job_delayed(job, queue)
                    elif status != STATUSES.WAITING:
                        self.job_skipped(job, queue)
                    else:
                        try:
                            self.job_started(job, queue)
                            job_result = self.callback(job, queue)
                        except Exception as e:
                            trace = None
                            if self.save_tracebacks:
                                trace = traceback.format_exc()
                            self.job_error(job, queue, e, trace)
                        else:
                            job._cached_status = job.status.hget()
                            if job._cached_status == STATUSES.DELAYED:
                                self.job_delayed(job, queue)
                            elif job._cached_status == STATUSES.CANCELED:
                                self.job_skipped(job, queue)
                            else:
                                self.job_success(job, queue, job_result)
                except Exception as e:
                    self.log('[%s] unexpected error: %s\n%s'
                        % (identifier, str(e), traceback.format_exc()), level='error')
                    try:
                        queue.errors.rpush(job.ident)
                    except Exception as e:
                        self.log('[%s] unable to add the error in the queue: %s\n%s'
                            % (identifier, str(e), traceback.format_exc()), level='error')

    def run_ended(self):
        """
        Called just after ending the run loop. Actually only do logging.
        """
        self.log('Run terminated, with %d loops (duration=%s)' % (
                                                self.num_loops, self.elapsed))

    def additional_error_fields(self, job, queue, exception, trace=None):
        """
        Return a dict with additional fields to set to a new Error subclass object
        """
        return {}

    def job_error(self, job, queue, exception, trace=None):
        """
        Called when an exception was raised during the execute call for a job.
        """
        to_be_requeued = not job.must_be_cancelled_on_error and self.requeue_times and self.requeue_times >= int(job.tries.hget() or 0)

        if not to_be_requeued:
            job.queued.delete()

        job.hmset(end=str(datetime.utcnow()), status=STATUSES.ERROR)
        queue.errors.rpush(job.ident)

        if self.save_errors:
            additional_fields = self.additional_error_fields(job, queue, exception)
            self.error_model.add_error(queue_name=queue._cached_name,
                                       job=job,
                                       error=exception,
                                       trace=trace,
                                       **additional_fields)

        self.log(self.job_error_message(job, queue, to_be_requeued, exception, trace), level='error')

        if hasattr(job, 'on_error'):
            job.on_error(queue, exception, trace)

        # requeue the job if needed
        if to_be_requeued:
            priority = queue.priority.hget()

            if self.requeue_priority_delta:
                priority = int(priority) + self.requeue_priority_delta

            self.requeue_job(job, queue, priority, delayed_for=self.requeue_delay_delta)

    def requeue_job(self, job, queue, priority, delayed_for=None):
        """
        Requeue a job in a queue with the given priority, possibly delayed
        """
        job.requeue(queue_name=queue._cached_name,
                    priority=priority,
                    delayed_for=delayed_for,
                    queue_model=self.queue_model)

        if hasattr(job, 'on_requeued'):
            job.on_requeued(queue)

        self.log(self.job_requeue_message(job, queue))

    def job_error_message(self, job, queue, to_be_requeued, exception, trace=None):
        """
        Return the message to log when a job raised an error
        """
        return '[%s|%s|%s] error: %s [%s]' % (queue._cached_name,
                             job.pk.get(),
                             job._cached_identifier,
                             str(exception),
                             'requeued' if to_be_requeued else 'NOT requeued')

    def job_requeue_message(self, job, queue):
        """
        Return the message to log when a job is requeued
        """
        priority, delayed_until = job.hmget('priority', 'delayed_until')

        msg = '[%s|%s|%s] requeued with priority %s'
        args = [queue._cached_name, job.pk.get(), job._cached_identifier, priority]

        if delayed_until:
            msg += ', delayed until %s'
            args.append(delayed_until)

        return msg % tuple(args)

    def job_success(self, job, queue, job_result):
        """
        Called just after an execute call was successful.
        job_result is the value returned by the callback, if any.
        """
        job.queued.delete()
        job.hmset(end=str(datetime.utcnow()), status=STATUSES.SUCCESS)
        queue.success.rpush(job.ident)
        self.log(self.job_success_message(job, queue, job_result))
        if hasattr(job, 'on_success'):
            job.on_success(queue, job_result)

    def job_success_message(self, job, queue, job_result):
        """
        Return the message to log when a job is successful
        """
        return '[%s|%s|%s] success, in %s' % (queue._cached_name, job.pk.get(),
                                           job._cached_identifier, job.duration)

    def job_started(self, job, queue):
        """
        Called just before the execution of the job
        """
        job.hmset(start=str(datetime.utcnow()), status=STATUSES.RUNNING)
        job.tries.hincrby(1)
        self.log(self.job_started_message(job, queue))
        if hasattr(job, 'on_started'):
            job.on_started(queue)

    def job_started_message(self, job, queue):
        """
        Return the message to log just befre the execution of the job
        """
        return '[%s|%s|%s] starting' % (queue._cached_name, job.pk.get(),
                                        job._cached_identifier)

    def job_skipped(self, job, queue):
        """
        Called if a job, before trying to run it, has not the "waiting" status,
        or, after run, if its status was set to "canceled"
        """
        job.queued.delete()
        self.log(self.job_skipped_message(job, queue), level='warning')
        if hasattr(job, 'on_skipped'):
            job.on_skipped(queue)

    def job_skipped_message(self, job, queue):
        """
        Return the message to log when a job was skipped
        """
        return '[%s|%s|%s] job skipped (current status: %s)' % (
                queue._cached_name,
                job.pk.get(),
                job._cached_identifier,
                STATUSES.by_value(job._cached_status, 'UNKNOWN'))

    def job_delayed(self, job, queue):
        """
        Called if a job, before trying to run it, has the "delayed" status, or,
        after run, if its status was set to "delayed"
        If delayed_until was not set, or is invalid, set it to 60sec in the future
        """
        delayed_until = job.delayed_until.hget()
        if delayed_until:
            try:
                delayed_until = compute_delayed_until(delayed_until=parse(delayed_until))
            except (ValueError, TypeError):
                delayed_until = None

        if not delayed_until:
            # by default delay it for 60 seconds
            delayed_until = compute_delayed_until(delayed_for=60)

        job.enqueue_or_delay(
            queue_name=queue._cached_name,
            delayed_until=delayed_until,
            queue_model=queue.__class__,
        )

        self.log(self.job_delayed_message(job, queue), level='warning')

        if hasattr(job, 'on_delayed'):
            job.on_delayed(queue)

    def job_delayed_message(self, job, queue):
        """
        Return the message to log when a job was delayed just before or during
        its execution
        """
        return '[%s|%s|%s] job delayed until %s' % (
                queue._cached_name,
                job.pk.get(),
                job._cached_identifier,
                job.delayed_until.hget())


class WorkerConfig(object):
    """
    The WorkerConfig class is aimed to be used in a script to run a worker.
    All options which can be accepted by the Worker class can be passed as
    arguments to the script (they are managed by optparse)
    In a script, simply instantiate a WorkerConfig object then call its
    "execute" method.
    """

    help = "Run a worker using redis-limpyd-jobs"

    option_list = (
        # pythonpath and worker-config are managed in the script, here just to
        # not throw error if found on command line but not in defined options
        make_option('--pythonpath', action='append',
            help='A directory to add to the Python path, e.g. --pythonpath=/my/module'),
        make_option('--worker-config', dest='worker_config',
            help='The worker config class to use, e.g. --worker-config=my.module.MyWorkerConfig, '
                  'default to limpyd_jobs.workers.WorkerConfig'),

        make_option('--print-options', action='store_true', dest='print_options',
            help='Print options used by the worker, e.g. --print-options'),
        make_option('--dry-run', action='store_true', dest='dry_run',
            help='Won\'t execute any job, just starts the worker and finish it immediatly, e.g. --dry-run'),

        make_option('--queues', action='store', dest='queues',
            help='Name of the Queues to handle, comma separated e.g. --queues=queue1,queue2'),

        make_option('--queue-model', action='store', dest='queue_model',
            help='Name of the Queue model to use (for all queues !), e.g. --queue-model=my.module.QueueModel'),
        make_option('--error-model', action='store', dest='error_model',
            help='Name of the Error model to use (for all errors !), e.g. --queue-model=my.module.ErrorModel'),

        make_option('--worker-class', action='store', dest='worker_class',
            help='Name of the Worker class to use, e.g. --worker-class=my.module.WorkerClass'),

        make_option('--callback', action='store', dest='callback',
            help='The callback to call for each job, e.g. --worker-class=my.module.callback'),

        make_option('--logger-name', action='store', dest='logger_name',
            help='The base name to use for logging, e.g. --logger-name="limpyd-jobs.my-job"'),
        make_option('--logger-level', action='store', dest='logger_level',
            help='The level to use for logging, e.g. --worker-class=INFO'),

        make_option('--save-errors', action='store_true', dest='save_errors',
            help='Save job errors in the Error model, e.g. --save-errors'),
        make_option('--no-save-errors', action='store_false', dest='save_errors',
            help='Do not save job errors in the Error model, e.g. --no-save-errors'),
        make_option('--save-tracebacks', action='store_true', dest='save_tracebacks',
            help='Save exception tracebacks on job error in the Error model, e.g. --save-tracebacks'),
        make_option('--no-save-tracebacks', action='store_false', dest='save_tracebacks',
            help='Do not save exception tracebacks on job error in the Error model, e.g. --no-save-tracebacks'),

        make_option('--max-loops', type='int', dest='max_loops',
            help='Max number of jobs to run, e.g. --max-loops=100'),
        make_option('--max-duration', type='int', dest='max_duration',
            help='Max duration of the worker, in seconds (None by default), e.g. --max-duration=3600'),

        make_option('--terminate-gracefuly', action='store_true', dest='terminate_gracefuly',
            help='Intercept SIGTERM and SIGINT signals to stop gracefuly, e.g. --terminate-gracefuly'),
        make_option('--no-terminate-gracefuly', action='store_false', dest='terminate_gracefuly',
            help='Do NOT intercept SIGTERM and SIGINT signals, so don\'t stop gracefuly, e.g. --no-terminate-gracefuly'),

        make_option('--timeout', type='int', dest='timeout',
            help='Max delay (seconds) to wait for a redis BLPOP call (0 for no timeout), e.g. --timeout=30'),

        make_option('--fetch-priorities-delay', type='int', dest='fetch_priorities_delay',
            help='Min delay (seconds) to wait before fetching new priority queues, e.g. --fetch-priorities-delay=20'),

        make_option('--fetch-delayed-delay', type='int', dest='fetch_delayed_delay',
            help='Min delay (seconds) to wait before updating delayed jobs, e.g. --fetch-delayed-delay=20'),

        make_option('--requeue-times', type='int', dest='requeue_times',
            help='Number of time to requeue a failing job (default to 0), e.g. --requeue-times=5'),

        make_option('--requeue-priority-delta', type='int', dest='requeue_priority_delta',
            help='Delta to add to the actual priority of a failing job to be requeued (default to -1, ie one level lower), e.g. --requeue-priority-delta=-2'),

        make_option('--requeue-delay-delta', type='int', dest='requeue_delay_delta',
            help='How much time (seconds) to delay a job to be requeued (default to 30), e.g. --requeue-delay-delta=15'),

        make_option('--database', action='store', dest='database',
            help='Redis database to use (host:port:db), e.g. --database=localhost:6379:15'),

        make_option('--no-title', action='store_false', dest='update_title', default=True,
            help="Do not update the title of the worker's process, e.g. --no-title"),
    )

    def __init__(self, argv=None):
        """
        Save arguments and program name
        """
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        self.manage_options()
        self.update_proc_title()

    def get_version(self):
        """
        Return the limpyd_jobs version. May be overriden for subclasses

        """
        return '(redis-limpyd-jobs %s, redis-limpyd %s)' % (
                                            limpyd_jobs_version, limpyd_version)

    def usage(self):
        """
        Return a brief description of how to use the worker based on self.help

        """
        usage = '%prog [options]'
        return '%s\n\n%s' % (usage, self.help)

    def create_parser(self):
        """
        Create and return the ``OptionParser`` which will be used to
        parse the arguments to the worker.

        """
        return OptionParser(prog=self.prog_name,
                            usage=self.usage(),
                            version='%%prog %s' % self.get_version(),
                            option_list=self.option_list)

    def manage_options(self):
        """
        Create a parser given the command-line arguments, creates a parser
        Return True if the programme must exit.
        """
        self.parser = self.create_parser()
        self.options, self.args = self.parser.parse_args(self.argv)

        self.do_imports()

        if self.options.callback and not callable(self.options.callback):
            self.parser.error('The callback is not callable')

        self.logger_level = None
        if self.options.logger_level:
            if self.options.logger_level.isdigit():
                self.options.logger_level = int(self.options.logger_level)
            else:
                try:
                    self.options.logger_level = getattr(logging, self.options.logger_level.upper())
                except:
                    self.parser.error('Invalid logger-level %s' % self.options.logger_level)

        if self.options.max_loops is not None and self.options.max_loops < 0:
            self.parser.error('The max-loops argument (%s) must be a <positive></positive> integer' % self.options.max_loops)

        if self.options.max_duration is not None and self.options.max_duration < 0:
            self.parser.error('The max-duration argument (%s) must be a positive integer' % self.options.max_duration)

        if self.options.timeout is not None and self.options.timeout < 0:
            self.parser.error('The timeout argument (%s) must be a positive integer (including 0)' % self.options.timeout)

        if self.options.fetch_priorities_delay is not None and self.options.fetch_priorities_delay <= 0:
            self.parser.error('The fetch-priorities-delay argument (%s) must be a positive integer' % self.options.fetch_priorities_delay)

        if self.options.fetch_delayed_delay is not None and self.options.fetch_delayed_delay <= 0:
            self.parser.error('The fetch-delayed-delay argument (%s) must be a positive integer' % self.options.fetch_delayed_delay)

        if self.options.requeue_times is not None and self.options.requeue_times < 0:
            self.parser.error('The requeue-times argument (%s) must be a positive integer (including 0)' % self.options.requeue_times)

        if self.options.requeue_delay_delta is not None and self.options.requeue_delay_delta < 0:
            self.parser.error('The rrequeue-delay-delta argument (%s) must be a positive integer (including 0)' % self.options.requeue_delay_delta)

        self.database_config = None
        if self.options.database:
            host, port, db = self.options.database.split(':')
            self.database_config = dict(host=host, port=int(port), db=int(db))

        self.update_title = self.options.update_title

    def do_import(self, name, default):
        """
        Import the given option (use default values if not defined on command line)
        """
        try:
            option = getattr(self.options, name)
            if option:
                klass = import_class(option)
            else:
                klass = default
            setattr(self.options, name, klass)
        except Exception as e:
            self.parser.error('Unable to import "%s": %s' % (name, e))

    def do_imports(self):
        """
        Import all importable options
        """
        self.do_import('worker_class', Worker)
        self.do_import('queue_model', self.options.worker_class.queue_model)
        self.do_import('error_model', self.options.worker_class.error_model)
        self.do_import('callback', self.options.worker_class.callback)

    def print_options(self):
        """
        Print all options as parsed by the script
        """
        options = []

        print("The script is running with the following options:")

        options.append(("dry_run", self.options.dry_run))

        options.append(("worker_config", self.__class__))

        database_config = self.database_config or \
                          self.options.queue_model.database.connection_settings
        options.append(("database", '%s:%s:%s' % (database_config['host'],
                                                  database_config['port'],
                                                  database_config['db'])))

        if self.options.worker_class is not None:
            options.append(("worker-class", self.options.worker_class))

        for name, value in options:
            print(" - %s = %s" % (name.replace('_', '-'), value))

        print("The worker will run with the following options:")
        for name in self.options.worker_class.parameters:
            option = getattr(self.worker, name)
            if name == 'callback' and \
                self.options.worker_class.execute == Worker.execute:
                option = '<jobs "run" method>'
            elif isinstance(option, (list, tuple, set)):
                option = ','.join(option)
            print(" - %s = %s" % (name.replace('_', '-'), option))

    def execute(self):
        """
        Main method to call to run the worker
        """
        self.prepare_models()
        self.prepare_worker()
        if self.options.print_options:
            self.print_options()
        self.run()

    def prepare_models(self):
        """
        If a database config ws given as argument, apply it to our models
        """
        if self.database_config:
            for model in (self.options.queue_model, self.options.error_model):
                model.database.reset(**self.database_config)

    def prepare_worker_options(self):
        """
        Prepare (and return as a dict) all options to be passed to the worker
        """
        worker_options = dict()
        for option_name in (self.options.worker_class.parameters):
            option = getattr(self.options, option_name)
            if option is not None:
                worker_options[option_name] = option
        return worker_options

    def prepare_worker(self):
        """
        Prepare the worker, ready to be launched: prepare options, create a
        log handler if none, and manage dry_run options
        """
        worker_options = self.prepare_worker_options()
        self.worker = self.options.worker_class(**worker_options)
        if self.update_title:
            self.worker._add_update_status_callback(self.update_proc_title)
            self.update_proc_title()

        if not self.worker.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                                        ' '.join(['[%(process)d]',
                                                  # '%(asctime)s,%(msecs).03d',
                                                  '%(asctime)s',
                                                  '(%(name)s)',
                                                  '%(levelname)-8s',
                                                  '%(message)s',
                                                 ])
                                        # , '%y.%m.%d:%H.%M.%S'
                                    ))
            self.worker.logger.addHandler(handler)

        if self.options.dry_run:
            self.worker.end_forced = True

    def run(self):
        """
        Simply run the worker by calling its "run" method
        """
        self.worker.run()

    def get_proc_title(self):
        """
        Create the title for the current process (set by `update_proc_title`)
        """
        has_worker = bool(getattr(self, 'worker', None))

        title_parts = [self.prog_name.replace('.py', ('#%s' % self.worker.id) if has_worker else '')]

        status = 'init'
        if has_worker and self.worker.status:
            status = self.worker.status
            if self.worker.end_forced:
                status += ' - ending'
        title_parts.append('[%s]' % status)

        if has_worker and self.worker.queues:
            title_parts.append('queues=%s' % ','.join(self.worker.queues))

        if has_worker and self.worker.status:
            # add infos about the main loop
            title_parts.append('loop=%s/%s' % (self.worker.num_loops, self.worker.max_loops))

            # and about the number of jobs to run
            title_parts.append('waiting=%s' % self.worker.count_waiting_jobs())

            # and about the number of delayed jobs
            title_parts.append('delayed=%s' % self.worker.count_delayed_jobs())

            # and about elapsed time
            if self.worker.start_date:
                duraiton_message = 'duration=%s'
                duration_args = (timedelta(seconds=int(round(total_seconds(self.worker.elapsed)))), )
                if self.worker.max_duration:
                    duraiton_message += '/%s'
                    duration_args += (self.worker.max_duration, )
                title_parts.append(duraiton_message % duration_args)

        return ' '.join(title_parts)

    def update_proc_title(self, worker=None):
        """
        Update the title of the process with useful informations
        """
        if not self.update_title:
            return
        setproctitle(self.get_proc_title())
