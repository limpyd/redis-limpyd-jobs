import logging
import signal
import sys
import os.path
from datetime import datetime
from optparse import make_option, OptionParser

from setproctitle import setproctitle

from limpyd import __version__ as limpyd_version
from limpyd.exceptions import ImplementationError, DoesNotExist

from limpyd_jobs import STATUSES
from limpyd_jobs.models import Queue, Job, Error

LOGGER_BASE_NAME = 'limpyd-jobs'
logger = logging.getLogger(LOGGER_BASE_NAME)


class Worker(object):
    # name of the worker (must be name of Queue objets)
    name = None

    # models to use
    queue_model = Queue
    job_model = Job
    error_model = Error

    # logging information
    logger_base_name = LOGGER_BASE_NAME + '.%s'  # will use self.name
    logger_level = logging.ERROR
    save_errors = True

    # maximum number of loops to run
    max_loops = 1000
    # max delay for blpop
    timeout = 30

    # we want to intercept SIGTERM and SIGINT signals, to stop gracefuly
    terminate_gracefuly = True

    def __init__(self, name=None, callback=None,
                 queue_model=None, job_model=None, error_model=None,
                 logger_base_name=None, logger_level=None, save_errors=None,
                 max_loops=None, terminate_gracefuly=None, timeout=None):
        """
        Create the worker by saving arguments, doing some checks, preparing
        logger and signals management, and getting queues keys.
        """
        if name is not None:
            self.name = name

        if not self.name:
            raise ImplementationError('The name of the worker is not defined')

        # save and check models to use
        if queue_model is not None:
            self.queue_model = queue_model
        self._assert_correct_model(self.queue_model, Queue, 'queue')
        if job_model is not None:
            self.job_model = job_model
        self._assert_correct_model(self.job_model, Job, 'job')
        if error_model is not None:
            self.error_model = error_model
        self._assert_correct_model(self.error_model, Error, 'error')

        # process other arguments
        self.callback = callback if callback is not None else self.execute
        if max_loops is not None:
            self.max_loops = max_loops
        if terminate_gracefuly is not None:
            self.terminate_gracefuly = terminate_gracefuly
        if save_errors is not None:
            self.save_errors = save_errors
        if timeout is not None:
            self.timeout = timeout

        # prepare logging
        if logger_base_name is not None:
            self.logger_base_name = logger_base_name
        if logger_level is not None:
            self.logger_level = logger_level
        self.set_logger()

        if self.terminate_gracefuly:
            self.handle_end_signal()

        self.keys = []  # list of redis keys to listen
        self.num_loops = 0  # loops counter
        self.end_forced = False  # set it to True in "execute" to force stop just after
        self.status = None  # is set to None/waiting/running by the worker
        self.end_signal_caught = False  # internaly set to True if end signal caught
        self.update_callbacks = []  # callbacks to call when status is updated

    @staticmethod
    def _assert_correct_model(model_to_check, model_reference, obj_name):
        """
        Helper that asserts the model_to_check is the model_reference or one of
        its subclasses. If not, raise an ImplementationError, using "obj_name"
        to describe the name of the argument.
        """
        if not issubclass(model_to_check, model_reference):
            raise ImplementationError('The %s model must be a subclass of %s' % (
                                      obj_name, model_reference.__name__))

    def handle_end_signal(self):
        """
        Catch some system signals to handle them internaly
        """
        signal.signal(signal.SIGTERM, self.catch_end_signal)
        signal.signal(signal.SIGINT, self.catch_end_signal)

    def set_logger(self):
        """
        Prepare the logger, based on self.logger_base_name and self.logger_level
        """
        self.logger = logging.getLogger(self.logger_base_name % self.name)
        self.logger.setLevel(self.logger_level)

    @property
    def id(self):
        """
        Return an identifier for the worker to use in logging
        """
        if not hasattr(self, '_id'):
            self._id = '%x' % id(self)
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
        return self.terminate_gracefuly and self.end_signal_caught \
            or self.num_loops >= self.max_loops or self.end_forced

    def set_status(self, status):
        """
        Save the new status and call all defined callbacks
        """
        self.status = status
        for callback in self.update_callbacks:
            callback(self)

    def add_update_callback(self, callback):
        """
        Add a callback to call when the status is updated
        """
        self.update_callbacks.append(callback)

    def remove_update_callback(self, callback):
        """
        Remove a callback from ones set to be called when the status is updated
        """
        self.update_callbacks.remove(callback)

    def wait_for_job(self):
        """
        Use a redis blocking list call to wait for a job, and return it.
        """
        blpop_result = self.connection.blpop(self.keys, self.timeout)
        if blpop_result is None:
            return None
        queue_redis_key, job_pk = blpop_result
        self.set_status('running')
        return self.get_queue(queue_redis_key), self.get_job(job_pk)

    def get_job(self, job_pk):
        """
        Return a job based on its primary key.
        """
        return self.job_model.get(job_pk)

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
        signal_name = dict((getattr(signal, n), n) for n in dir(signal)
                        if n.startswith('SIG') and '_' not in n).get(signum, signum)

        if self.status == 'running':
            self.log('Catched %s signal: stopping after current job' % signal_name,
                     level='critical')
        else:
            self.log('Catched %s signal: stopping right now' % signal_name,
                     level='critical')

        self.end_signal_caught = self.end_forced = True

    def execute(self, job, queue):
        """
        The method to run for a job when got bak from the queue if no callback
        was defined on __init__.
        The optional return value of this function will be passed to the
        job_success method.
        """
        raise NotImplementedError('You must implement your own action')

    def update_keys(self):
        """
        Update the redis keys to listen for new jobs.
        """
        self.keys = self.queue_model.get_keys(self.name)

    def count_waiting_jobs(self):
        """
        Return the number of all jobs waiting in queues managed by the worker
        """
        return self.queue_model.count_waiting_jobs(self.name)

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
            raise ImplementationError('This worker run is already terminated')

        self.set_status('starting')

        self.update_keys()
        if not self.keys:
            self.log('No queues with the name %s.' % self.name, level='error')
            self.set_status('aborted')
            return

        self.run_started()

        while not self.must_stop():
            self.set_status('waiting')
            try:
                queue_and_job = self.wait_for_job()
                if queue_and_job is None:
                    # timeout for blpop
                    continue
                queue, job = queue_and_job
            except Exception, e:
                self.log('Unable to get job: %s' % str(e), level='error')
            else:
                self.num_loops += 1
                identifier = 'pk:%s' % job.pk.get()  # default if failure
                try:
                    self.set_status('running')
                    identifier, status = job.hmget('identifier', 'status')
                     # some cache, don't count on it on subclasses
                    job._identifier = identifier
                    job._status = status

                    if status != STATUSES.WAITING:
                        self.job_skipped(job, queue)
                    else:
                        try:
                            self.job_started(job, queue)
                            job_result = self.callback(job, queue)
                        except Exception, e:
                            self.job_error(job, queue, e)
                        else:
                            self.job_success(job, queue, job_result)
                except Exception, e:
                    self.log('[%s] unexpected error: %s' % (identifier, str(e)),
                             level='error')

        self.set_status('terminated')
        self.run_ended()

    def run_ended(self):
        """
        Called just after ending the run loop. Actually only do logging.
        """
        self.log('Run terminated, with %d loops.' % self.num_loops)

    def additional_error_fields(self, job, queue, exception):
        """
        Return a dict with additional fields to set to a new Error subclass object
        """
        return {}

    def job_error(self, job, queue, exception, message=None):
        """
        Called when an exception was raised during the execute call for a job.
        """
        job.hmset(end=str(datetime.utcnow()), status=STATUSES.ERROR)
        queue.errors.rpush(job.pk.get())

        if self.save_errors:
            additional_fields = self.additional_error_fields(job, queue, exception)
            self.error_model.add_error(queue_name=queue.name.hget(),
                                       identifier=job._identifier,
                                       error=exception,
                                       **additional_fields)

        if not message:
            message = '[%s] error: %s' % (job._identifier, str(exception))
        self.log(message, level='error')

    def job_success(self, job, queue, job_result, message=None):
        """
        Called just after an execute call was successful.
        job_result is the value returned by the callback, if any.
        """
        job.hmset(end=str(datetime.utcnow()), status=STATUSES.SUCCESS)
        queue.success.rpush(job.pk.get())

        if not message:
            message = '[%s] success, in %s' % (job._identifier, job.duration)
        self.log(message)

    def job_started(self, job, queue, message=None):
        """
        Called just before the execution of the job
        """
        job.hmset(start=str(datetime.utcnow()), status=STATUSES.RUNNING)

        if not message:
            message = '[%s] starting' % job._identifier
        self.log(message)

    def job_skipped(self, job, queue, message=None):
        """
        Called if a job can't be run: canceled, already running or done.
        """
        if not message:
            message = '[%s] job skipped (current status: %s)' % (
                    STATUSES.by_value(job._status, 'UNKNOWN'), job._identifier)
        self.log(message, level='warning')


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
            help='Print options as parsed by the script, e.g. --print-options'),
        make_option('--dry-run', action='store_true', dest='dry_run',
            help='Won\'t execute any job, just starts the worker and finish it immediatly, e.g. --dry-run'),

        make_option('--name', action='store', dest='name',
            help='Name of the Queues to handle e.g. --name=my-queue-name'),

        make_option('--job-model', action='store', dest='job_model',
            help='Name of the Job model to use, e.g. --job-model=my.module.JobModel'),
        make_option('--queue-model', action='store', dest='queue_model',
            help='Name of the Queue model to use, e.g. --queue-model=my.module.QueueModel'),
        make_option('--error-model', action='store', dest='error_model',
            help='Name of the Error model to use, e.g. --queue-model=my.module.ErrorModel'),

        make_option('--worker-class', action='store', dest='worker_class',
            help='Name of the Worker class to use, e.g. --worker-class=my.module.WorkerClass'),

        make_option('--callback', action='store', dest='callback',
            help='The callback to call for each job, e.g. --worker-class=my.module.callback'),

        make_option('--logger-base-name', action='store', dest='logger_base_name',
            help='The base name to use for logging, e.g. --logger-base-name="limpyd-jobs.%s"'),
        make_option('--logger-level', action='store', dest='logger_level',
            help='The level to use for logging, e.g. --worker-class=INFO'),

        make_option('--save-errors', action='store_true', dest='save_errors',
            help='Save job errors in the Error model, e.g. --save-errors'),
        make_option('--no-save-errors', action='store_false', dest='save_errors',
            help='Do not save job errors in the Error model, e.g. --no-save-errors'),

        make_option('--max-loops', type='int', dest='max_loops',
            help='Max number of jobs to run, e.g. --max-loops=100'),

        make_option('--terminate-gracefuly', action='store_true', dest='terminate_gracefuly',
            help='Intercept SIGTERM and SIGINT signals to stop gracefuly, e.g. --terminate-gracefuly'),
        make_option('--no-terminate-gracefuly', action='store_false', dest='terminate_gracefuly',
            help='Do NOT intercept SIGTERM and SIGINT signals, so don\'t stop gracefuly, e.g. --no-terminate-gracefuly'),

        make_option('--timeout', type='int', dest='timeout',
            help='Max delay (seconds) to wait for a redis BLPOP call (0 for no timeout), e.g. --timeout=30'),

        make_option('--database', action='store', dest='database',
            help='Redis database to use (host:port:db), e.g. --database=localhost:6379:15'),

        make_option('--no-title', action='store_false', dest='update_title', default=True,
            help="Do not update the title of the worker's process, e.g. --no-title"),
    )

    default_classes = {
        'job_model': Job,
        'queue_model': Queue,
        'error_model': Error,
        'worker_class': Worker,
        'callback': None,
    }

    worker_options = ('name', 'job_model', 'queue_model', 'error_model',
                      'callback', 'logger_base_name', 'logger_level',
                      'save_errors', 'max_loops', 'terminate_gracefuly', 'timeout')

    @staticmethod
    def _import_module(module_uri):
        return __import__(module_uri, {}, {}, [''])

    @staticmethod
    def import_class(class_uri):
        """
        Import a class by string 'from.path.module.class'
        """
        try:
            from importlib import import_module
            callback = import_module
        except ImportError:
            callback = WorkerConfig._import_module

        parts = class_uri.split('.')
        class_name = parts.pop()
        module_uri = '.'.join(parts)

        try:
            module = callback(module_uri)
        except ImportError:
            # maybe we are still in a module, test going up one level
            module = WorkerConfig.import_class(module_uri)

        return getattr(module, class_name)

    def __init__(self, argv=None):
        """
        Save arguments and program name
        """
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        self.manage_options()
        if self.options.print_options:
            self.print_options()
        self.update_proc_title()

    def get_version(self):
        """
        Return the limpyd_jobs version. May be overriden for subclasses

        """
        return '(redis-limpyd-jobs %s)' % limpyd_version

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

        if self.argv[1:] in (['--help'], ['-h'], ['--version']):
            # OptionParser already takes care of printing help and version.
            # We should never pass here
            return True

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
            self.parser.error('The max-loops argument (%s) must be a positive integer' % self.options.max_loops)

        if self.options.timeout is not None and self.options.timeout < 0:
            self.parser.error('The timeout argument (%s) must be a positive integer (including 0)' % self.options.timeout)

        self.database_config = None
        if self.options.database:
            host, port, db = self.options.database.split(':')
            self.database_config = dict(host=host, port=int(port), db=int(db))

        self.update_title = self.options.update_title

    def do_import(self, name):
        """
        Import the given option (use default values if not defined on command line)
        """
        option = getattr(self.options, name)
        if option:
            klass = WorkerConfig.import_class(option)
        else:
            klass = self.default_classes[name]
        setattr(self.options, name, klass)

    def do_imports(self):
        """
        Import all importable options
        """
        for option in self.default_classes.keys():
            try:
                self.do_import(option)
            except Exception, e:
                self.parser.error('Unable to import "%s": %s' % (option, e))

    def print_options(self):
        """
        Print all options as parsed by the script
        """
        options = []

        if self.__class__ != WorkerConfig:
            options.append(("worker_config", '%s.%s' % (self.__class__.__module__,
                                                        self.__class__.__name__)))

        if self.database_config:
            options.append(("database", '%s:%s:%s' % (self.database_config['host'],
                                                      self.database_config['port'],
                                                      self.database_config['db'])))

        if self.options.dry_run:
            options.append(("dry_run", self.options.dry_run))

        for option_name in self.worker_options:
            option = getattr(self.options, option_name)
            if option is not None:
                options.append((option_name.replace('_', '-'), option))
            # special case for worker_class which is not a worker option
            if option_name == 'error_model' and self.options.worker_class is not None:
                options.append(("worker-class", self.options.worker_class))

        if options:
            print "The worker will run with the following options:"
            for name, value in options:
                print " - %s = %s" % (name, value)

    def execute(self):
        """
        Main method to call to run the worker
        """
        self.prepare_models()
        self.prepare_worker()
        self.run()

    def prepare_models(self):
        """
        If a database config ws given as argument, apply it to our models
        """
        if self.database_config:
            for model in (self.options.job_model, self.options.queue_model,
                                                    self.options.error_model):
                model.database.reset(**self.database_config)

    def prepare_worker_options(self):
        """
        Prepare (and return as a dict) all options to be passed to the worker
        """
        worker_options = dict()
        for option_name in (self.worker_options):
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
            self.worker.add_update_callback(self.update_proc_title)
            self.update_proc_title()

        if not self.worker.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
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

        if has_worker and self.worker.name:
            title_parts.append('queue=%s' % self.worker.name)

        if has_worker and self.worker.status:
            # add infos about the main loop
            title_parts.append('loop=%s/%s' % (self.worker.num_loops, self.worker.max_loops))

            # and about the number of jobs to run
            title_parts.append('waiting-jobs=%s' % self.worker.count_waiting_jobs())

        return ' '.join(title_parts)

    def update_proc_title(self, worker=None):
        """
        Update the title of the process with useful informations
        """
        if not self.update_title:
            return
        setproctitle(self.get_proc_title())
