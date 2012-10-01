import logging
import signal
from datetime import datetime

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

    # we want to intercept SIGTERM and SIGINT signals, to stop gracefuly
    terminate_gracefuly = True

    def __init__(self, name=None, callback=None,
                 queue_model=None, job_model=None, error_model=None,
                 logger_base_name=None, logger_level=None, save_errors=None,
                 max_loops=None, terminate_gracefuly=None):
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

        # prepare logging
        if logger_base_name is not None:
            self.logger_base_name = logger_base_name
        if logger_level is not None:
            self.logger_level = logger_level
        self.set_logger()

        if self.terminate_gracefuly:
            self.handle_end_signal()

        self.keys = self.queue_model.get_keys(self.name)

        self.num_loops = 0  # loops counter
        self.end_forced = False  # set it to True in "execute" to force stop just after
        self.status = None  # is set to None/waiting/running by the worker
        self.end_signal_catched = False  # internaly set to True if end signal catched

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
    def connection(self):
        """
        Return the redis connection to use.
        """
        return self.queue_model.get_connection()

    def must_stop(self):
        """
        Return True if the worker must stop when the current loop is over.
        """
        return self.terminate_gracefuly and self.end_signal_catched \
            or self.num_loops >= self.max_loops or self.end_forced

    def wait_for_job(self):
        """
        Use a redis blocking list call to wait for a job, and return it.
        """
        queue_redis_key, job_pk = self.connection.blpop(self.keys)
        self.status = 'running'
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
        When a SIGINT/SIGTERM signal is catched, this method is called, asking
        for the worker to terminate as soon as possible.
        """
        signal_name = dict((getattr(signal, n), n) for n in dir(signal)
                        if n.startswith('SIG') and '_' not in n).get(signum, signum)

        if self.status == 'running':
            self.logger.critical('Catched %s signal: stopping after current job' % signal_name)
        else:
            self.logger.critical('Catched %s signal: stopping right now' % signal_name)

        self.end_signal_catched = True

    def execute(self, job, queue):
        """
        The method to run for a job when got bak from the queue if no callback
        was defined on __init__.
        """
        raise NotImplementedError('You must implement your own action')

    def run_started(self):
        """
        Called just before starting to wait for jobs. Actually only do logging.
        """
        self.logger.info('Run started.')

    def run(self):
        """
        The main method of the worker. Will ask redis for list items via
        blocking calls, get jobs from them, try to execute these jobs, and end
        when needed.
        """
        # if status is not None, we already had a run !
        if self.status:
            raise ImplementationError('This worker run is already terminated')

        self.run_started()
        while not self.must_stop():
            self.num_loops += 1
            self.status = 'waiting'
            try:
                queue, job = self.wait_for_job()
            except Exception, e:
                self.logger.error('Unable to get job: %s' % str(e))
            else:
                identifier, status = job.hmget('identifier', 'status')

                if status == STATUSES.CANCELED:
                    self.logger.warning('[%s] job previously canceled' % identifier)

                else:
                    try:
                        self.logger.info('[%s] starting' % identifier)
                        job.start.hset(str(datetime.utcnow()))
                        self.callback(job, queue)
                    except Exception, e:
                        self.job_error(job, queue, e)
                    else:
                        job.end.hset(str(datetime.utcnow()))
                        self.job_success(job, queue)

        self.status = 'terminated'
        self.run_ended()

    def run_ended(self):
        """
        Called just after ending the run loop. Actually only do logging.
        """
        self.logger.info('Run terminated, with %d loops.' % self.num_loops)

    def additional_error_fields(self, job, queue, exception):
        """
        Return a dict with additional fields to set to a new Error subclass object
        """
        return {}

    def job_error(self, job, queue, exception, message=None):
        """
        Called when an exception was raised during the execute call for a job.
        """
        job.status.hset(STATUSES.ERROR)
        queue.errors.rpush(job.get_pk())

        if self.save_errors:
            additional_fields = self.additional_error_fields(job, queue, exception)
            self.error_model.add_error(queue_name=queue.name.hget(),
                                       identifier=job.identifier.hget(),
                                       error=exception,
                                       **additional_fields)

        if not message:
            message = str(exception)
        self.logger.error(message)

    def job_success(self, job, queue, message=None):
        """
        Called just after an execute call was successful.
        """
        job.status.hset(STATUSES.SUCCESS)
        queue.success.rpush(job.get_pk())

        if not message:
            message = '[%s] success, in %ss)' % (job.identifier.hget(), job.duration)
        self.logger.info(message)
