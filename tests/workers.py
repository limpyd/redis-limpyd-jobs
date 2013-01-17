import logging
import threading
import time
import signal
import sys
from StringIO import StringIO

from limpyd import __version__ as limpyd_version, fields
from limpyd.contrib.database import PipelineDatabase
from limpyd.exceptions import ImplementationError, DoesNotExist

from limpyd_jobs.models import Queue, Job, Error
from limpyd_jobs.workers import Worker, WorkerConfig
from limpyd_jobs import STATUSES

from .base import LimpydBaseTest


class WorkerArgumentsTest(LimpydBaseTest):
    class TestQueue(Queue):
        namespace = 'WorkerArgumentsTest'

    class TestJob(Job):
        namespace = 'WorkerArgumentsTest'

    class TestError(Error):
        namespace = 'WorkerArgumentsTest'

    def test_name_should_be_mandatory_if_not_defined_in_class(self):
        with self.assertRaises(ImplementationError):
            Worker()

        worker = Worker(name='testfoo')
        self.assertEqual(worker.name, 'testfoo')

        class TestWorker(Worker):
            name = 'testbar'

        worker = TestWorker()
        self.assertEqual(worker.name, 'testbar')

    def test_default_worker_should_use_default_params(self):
        worker = Worker(name='test')

        self.assertEqual(worker.name, 'test')
        self.assertEqual(worker.queue_model, Queue)
        self.assertEqual(worker.job_model, Job)
        self.assertEqual(worker.error_model, Error)
        self.assertEqual(worker.callback, worker.execute)
        self.assertEqual(worker.logger, logging.getLogger('limpyd-jobs.test'))
        self.assertEqual(worker.logger.level, logging.ERROR)
        self.assertEqual(worker.max_loops, 1000)
        self.assertEqual(worker.save_errors, True)
        self.assertEqual(worker.terminate_gracefuly, True)
        self.assertEqual(worker.timeout, 30)

    def test_worker_arguements_should_be_saved(self):
        def callback(job, queue):
            pass

        worker = Worker(
                    name='test',
                    queue_model=WorkerArgumentsTest.TestQueue,
                    job_model=WorkerArgumentsTest.TestJob,
                    error_model=WorkerArgumentsTest.TestError,
                    callback=callback,
                    logger_base_name='limpyd-jobs.worker.%s',
                    logger_level=logging.DEBUG,
                    max_loops=500,
                    terminate_gracefuly=False,
                    save_errors=False,
                    timeout=20
                )

        self.assertEqual(worker.name, 'test')
        self.assertEqual(worker.queue_model, WorkerArgumentsTest.TestQueue)
        self.assertEqual(worker.job_model, WorkerArgumentsTest.TestJob)
        self.assertEqual(worker.error_model, WorkerArgumentsTest.TestError)
        self.assertEqual(worker.callback, callback)
        self.assertEqual(worker.logger, logging.getLogger('limpyd-jobs.worker.test'))
        self.assertEqual(worker.logger.level, logging.DEBUG)
        self.assertEqual(worker.max_loops, 500)
        self.assertEqual(worker.save_errors, False)
        self.assertEqual(worker.terminate_gracefuly, False)
        self.assertEqual(worker.timeout, 20)

    def test_worker_subclass_attributes_should_be_used(self):
        class TestWorker(Worker):
            name = 'test'
            queue_model = WorkerArgumentsTest.TestQueue
            job_model = WorkerArgumentsTest.TestJob
            error_model = WorkerArgumentsTest.TestError
            max_loops = 500
            logger_base_name = 'limpyd-jobs.worker.%s'
            logger_level = logging.DEBUG
            terminate_gracefuly = False
            save_errors = False
            timeout = 20

        worker = TestWorker()

        self.assertEqual(worker.name, 'test')
        self.assertEqual(worker.queue_model, WorkerArgumentsTest.TestQueue)
        self.assertEqual(worker.job_model, WorkerArgumentsTest.TestJob)
        self.assertEqual(worker.error_model, WorkerArgumentsTest.TestError)
        self.assertEqual(worker.logger, logging.getLogger('limpyd-jobs.worker.test'))
        self.assertEqual(worker.logger.level, logging.DEBUG)
        self.assertEqual(worker.max_loops, 500)
        self.assertEqual(worker.save_errors, False)
        self.assertEqual(worker.terminate_gracefuly, False)
        self.assertEqual(worker.timeout, 20)

    def test_worker_subclass_attributes_should_be_overriden_by_arguments(self):
        class OtherTestQueue(Queue):
            namespace = 'test_worker_subclass_attribute_should_be_overriden_by_arguments'

        class OtherTestJob(Job):
            namespace = 'test_worker_subclass_attribute_should_be_overriden_by_arguments'

        class OtherTestError(Error):
            namespace = 'test_worker_subclass_attribute_should_be_overriden_by_arguments'

        class TestWorker(Worker):
            name = 'test'
            queue_model = WorkerArgumentsTest.TestQueue
            job_model = WorkerArgumentsTest.TestJob
            error_model = WorkerArgumentsTest.TestError
            max_loops = 500
            logger_base_name = 'limpyd-jobs.worker.%s'
            logger_level = logging.DEBUG
            terminate_gracefuly = False
            save_errors = False
            timeout = 20

        worker = Worker(
                    name='testfoo',
                    queue_model=OtherTestQueue,
                    job_model=OtherTestJob,
                    error_model=OtherTestError,
                    logger_base_name='limpyd-jobs.workerfoo.%s',
                    logger_level=logging.INFO,
                    max_loops=200,
                    terminate_gracefuly=True,
                    save_errors=True,
                    timeout = 40
                )

        self.assertEqual(worker.name, 'testfoo')
        self.assertEqual(worker.queue_model, OtherTestQueue)
        self.assertEqual(worker.job_model, OtherTestJob)
        self.assertEqual(worker.error_model, OtherTestError)
        self.assertEqual(worker.logger, logging.getLogger('limpyd-jobs.workerfoo.testfoo'))
        self.assertEqual(worker.logger.level, logging.INFO)
        self.assertEqual(worker.max_loops, 200)
        self.assertEqual(worker.save_errors, True)
        self.assertEqual(worker.terminate_gracefuly, True)
        self.assertEqual(worker.timeout, 40)

    def test_bad_model_should_be_rejected(self):
        class FooBar(object):
            pass

        with self.assertRaises(ImplementationError):
            Worker('test', queue_model=FooBar)
        with self.assertRaises(ImplementationError):
            Worker('test', job_model=FooBar)
        with self.assertRaises(ImplementationError):
            Worker('test', error_model=FooBar)


class WorkerRunTest(LimpydBaseTest):

    def tests_worker_should_handle_many_queues(self):
        queue1 = Queue(name='test', priority=1)
        queue2 = Queue(name='test', priority=2)
        worker = Worker('test')
        worker.update_keys()

        self.assertEqual(worker.keys, [queue2.waiting.key, queue1.waiting.key])

    def test_worker_should_stop_after_max_loops(self):
        Job.add_job('job:1', 'test')
        worker = Worker('test', max_loops=1)  # one loop to run only one job
        worker.run()
        self.assertEqual(worker.num_loops, 1)
        self.assertEqual(worker.status, 'terminated')

    def test_run_started_method_should_be_called(self):
        class TestWorker(Worker):
            passed = False

            def run_started(self):
                super(TestWorker, self).run_started()
                self.passed = True

        Job.add_job('job:1', 'test')
        worker = TestWorker('test', max_loops=1)  # one loop to run only one job
        worker.run()

        self.assertEqual(worker.passed, True)

    def test_must_stop_method_should_work(self):
        worker = Worker('test')
        self.assertEqual(worker.must_stop(), False)
        worker.num_loops = worker.max_loops
        self.assertEqual(worker.must_stop(), True)
        worker.num_loops = 0
        worker.end_forced = True
        self.assertEqual(worker.must_stop(), True)
        worker.end_forced = False
        worker.end_signal_caught = True
        self.assertEqual(worker.must_stop(), True)
        worker.terminate_gracefuly = False
        self.assertEqual(worker.must_stop(), False)

    def test_wait_for_job_should_respond_with_queue_and_job_when_a_queue_is_not_empty(self):
        queue = Queue.get_queue('test')
        worker = Worker('test')
        worker.test_content = None

        class Thread(threading.Thread):
            def run(self):
                worker.update_keys()
                queue, job = worker.wait_for_job()
                worker.test_content = queue, job

        # start listening
        thread = Thread()
        thread.start()
        time.sleep(0.1)

        # no job, nothing received
        self.assertIsNone(worker.test_content)

        # add a job
        job = Job.add_job(queue_name='test', identifier='job:1')
        time.sleep(0.1)

        # info should be received
        self.assertEqual(worker.test_content[0].pk.get(), queue.pk.get())
        self.assertTrue(isinstance(worker.test_content[0], Queue))
        self.assertEqual(worker.test_content[1].pk.get(), job.pk.get())
        self.assertTrue(isinstance(worker.test_content[1], Job))

        # job must no be in the queue anymore
        self.assertNotIn(job.pk.get(), queue.waiting.lmembers())

    def test_get_job_method_should_return_a_job_based_on_its_pk(self):
        worker = Worker('test')
        job = Job.add_job(queue_name='test', identifier='job:1')

        # a job...
        test_job = worker.get_job(job.pk.get())
        self.assertTrue(isinstance(test_job, Job))
        self.assertEqual(test_job.pk.get(), job.pk.get())

        # not a job...
        with self.assertRaises(DoesNotExist):
            worker.get_job('foo')

    def test_get_queue_method_should_return_a_queue_based_on_its_waiting_field_key(self):
        worker = Worker('test')
        queue = Queue.get_queue('test')

        # a queue...
        test_queue = worker.get_queue(queue.waiting.key)
        self.assertTrue(isinstance(test_queue, Queue))
        self.assertEqual(test_queue.pk.get(), queue.pk.get())

        # a non_existing_queue
        fake_queue_key = test_queue.waiting.key.replace('1', '2')
        with self.assertRaises(DoesNotExist):
            worker.get_queue(fake_queue_key)

        # not a queue...
        with self.assertRaises(DoesNotExist):
            worker.get_queue('foo')

    def test_job_started_method_should_be_called(self):
        class TestWorker(Worker):
            passed = False

            def execute():
                pass

            def job_started(self, job, queue):
                super(TestWorker, self).job_started(job, queue)
                self.passed = True

        Job.add_job(identifier='job:1', queue_name='test')
        worker = TestWorker(name='test', max_loops=1)
        worker.run()

        self.assertTrue(worker.passed)

    def test_callback_should_be_called(self):
        result = {}

        def callback(job, queue):
            result['job'] = job.pk.get()
            result['queue'] = queue.pk.get()

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', callback=callback, max_loops=1)
        worker.run()

        self.assertEqual(result, {'job': job.pk.get(), 'queue': queue.pk.get()})

    def test_job_success_method_should_be_called(self):
        class TestWorker(Worker):
            passed = None

            def execute(self, job, queue):
                return 42

            def job_success(self, job, queue, job_result, message=None):
                super(TestWorker, self).job_success(job, queue, job_result, message)
                self.passed = job_result

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = TestWorker(name='test', max_loops=1)
        worker.run()

        self.assertEqual(job.status.hget(), STATUSES.SUCCESS)
        self.assertIn(job.pk.get(), queue.success.lmembers())
        self.assertEqual(worker.passed, 42)

    def test_job_error_method_should_be_called(self):
        class ExceptionWithCode(Exception):
            def __init__(self, message, code):
                super(ExceptionWithCode, self).__init__(message)
                self.message = message
                self.code = code

        def callback(job, queue):
            raise ExceptionWithCode('foobar', 42)

        job1 = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', max_loops=1)  # no callback
        worker.run()

        self.assertEqual(job1.status.hget(), STATUSES.ERROR)
        self.assertIn(job1.pk.get(), queue.errors.lmembers())
        self.assertEqual(len(Error.collection()), 1)
        error = Error.get(identifier='job:1')
        self.assertEqual(error.message.hget(), 'You must implement your own action')
        self.assertEqual(error.code.hget(), None)

        job2 = Job.add_job(identifier='job:2', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', max_loops=1, callback=callback)  # callback with exception
        worker.run()

        self.assertEqual(job2.status.hget(), STATUSES.ERROR)
        self.assertIn(job2.pk.get(), queue.errors.lmembers())
        self.assertEqual(len(Error.collection()), 2)
        error = Error.get(identifier='job:2')
        self.assertEqual(error.message.hget(), 'foobar')
        self.assertEqual(error.code.hget(), '42')

    def test_error_model_with_additional_fields(self):
        class TestError(Error):
            foo = fields.InstanceHashField()

        class TestWorker(Worker):
            error_model = TestError

            def additional_error_fields(self, job, queue, exception):
                return {
                    'foo': 'Error on queue %s for job %s' % (
                        job.pk.get(), queue.pk.get()
                    )
                }

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = TestWorker(name='test', max_loops=1)  # no callback
        worker.run()

        error = TestError.get(identifier='job:1')
        attended_foo = 'Error on queue %s for job %s' % (job.pk.get(), queue.pk.get())
        self.assertEqual(error.foo.hget(), attended_foo)

    def test_run_ended_method_should_be_called(self):
        class TestWorker(Worker):
            passed = False

            def run_ended(self):
                super(TestWorker, self).run_ended()
                self.passed = True

        Job.add_job('job:1', 'test')
        worker = TestWorker('test', max_loops=1)  # one loop to run only one job
        worker.run()

        self.assertEqual(worker.passed, True)

    def test_send_stop_signal_should_stop_worker(self):
        class TestWorker(Worker):
            def execute(self, job, queue):
                """
                Simulate a signal by directly calling the signal handler
                """
                self.catch_end_signal(signal.SIGINT, None)

        Job.add_job('job:1', 'test')
        Job.add_job('job:2', 'test')
        queue = Queue.get_queue('test')
        worker = TestWorker('test', max_loops=2)
        worker.run()

        self.assertEqual(worker.end_signal_caught, True)
        self.assertEqual(queue.success.llen(), 1)
        self.assertEqual(queue.waiting.llen(), 1)

    def test_a_worker_should_run_only_one_time(self):

        worker = Worker('test', max_loops=2)
        worker.run()

        with self.assertRaises(ImplementationError):
            worker.run()

    def test_job_skipped_method_should_be_called(self):
        class TestWorker(Worker):
            passed = False

            def execute():
                pass

            def job_skipped(self, job, queue):
                super(TestWorker, self).job_skipped(job, queue)
                self.passed = job._status

        for status in ('CANCELED', 'RUNNING', 'SUCCESS', 'ERROR'):
            job = Job.add_job(identifier='job:1', queue_name='test')
            job.status.hset(STATUSES[status])
            worker = TestWorker(name='test', max_loops=1)
            worker.run()
            self.assertEqual(worker.passed, STATUSES[status])
            # job ignored: keep its status, and removed from queue
            queue = Queue.get_queue('test')
            self.assertEqual(queue.waiting.llen(), 0)
            self.assertEqual(queue.success.llen(), 0)
            self.assertEqual(queue.errors.llen(), 0)
            self.assertEqual(job.status.hget(), STATUSES[status])

    def test_worker_without_queues_should_stop(self):
        worker = Worker(name='test', max_loops=1)
        worker.run()
        self.assertEqual(worker.num_loops, 0)

    def test_blpop_timeout(self):
        class TestWorker(Worker):
            def wait_for_job(self):
                result = super(TestWorker, self).wait_for_job()
                if result is None:
                    # force end to quit quickly
                    self.end_forced = True
                return result

        Queue.get_queue('test')

        # test specific methods
        worker = Worker(name='test', timeout=1)
        worker.update_keys()
        test_value = worker.wait_for_job()
        self.assertIsNone(test_value)

        # test whole run
        worker = TestWorker(name='test', timeout=1)
        worker.run()
        self.assertEqual(worker.num_loops, 0)


class WorkerConfigBaseTest(LimpydBaseTest):

    def setUp(self):
        super(WorkerConfigBaseTest, self).setUp()
        self.old_stdout = sys.stdout
        sys.stdout = self.stdout = StringIO()
        self.old_stderr = sys.stderr
        self.stderr = StringIO()

    def tearDown(self):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        super(WorkerConfigBaseTest, self).tearDown()

    def mkargs(self, args=None):
        if args is None:
            args = ''
        return ['test-script'] + args.split(' ')


class WorkerConfigArgumentsTest(WorkerConfigBaseTest):
    class JobModel(Job):
        namespace = 'WorkerConfigArgumentsTest'

    class QueueModel(Queue):
        namespace = 'WorkerConfigArgumentsTest'

    class ErrorModel(Error):
        namespace = 'WorkerConfigArgumentsTest'

    class WorkerClass(Worker):
        pass

    @staticmethod
    def callback(job, queue):
        pass

    not_a_callback = True

    def test_help_argument(self):
        with self.assertSystemExit(in_stdout='Usage: '):
            WorkerConfig(self.mkargs('--help'))

    def test_version_argument(self):
        with self.assertSystemExit(in_stdout='(redis-limpyd-jobs %s)' % limpyd_version):
            WorkerConfig(self.mkargs('--version'))

    def test_print_options_arguments(self):
        class TestWorkerConfig(WorkerConfig):
            pass

        TestWorkerConfig(self.mkargs('--print-options --name=foo --dry-run --database=localhost:6379:15'))
        out = self.stdout.getvalue()
        self.assertTrue(out.startswith('The worker will run with the following options:'))
        self.assertIn('name = foo', out)
        self.assertIn('dry_run = True', out)
        self.assertIn('database = localhost:6379:15', out)
        self.assertIn('worker_config = tests.workers.TestWorkerConfig', out)

    def test_dryrun_argument(self):
        conf = WorkerConfig(self.mkargs('--dry-run'))
        self.assertTrue(conf.options.dry_run)

    def test_name_argument(self):
        conf = WorkerConfig(self.mkargs('--name=foo'))
        self.assertEqual(conf.options.name, 'foo')

    def test_job_model_argument(self):
        conf = WorkerConfig(self.mkargs('--job-model=tests.workers.WorkerConfigArgumentsTest.JobModel'))
        self.assertEqual(conf.options.job_model, self.JobModel)

        with self.assertSystemExit(in_stderr='Unable to import "job_model"'):
            WorkerConfig(self.mkargs('--job-model=foo.bar'))

    def test_queue_model_argument(self):
        conf = WorkerConfig(self.mkargs('--queue-model=tests.workers.WorkerConfigArgumentsTest.QueueModel'))
        self.assertEqual(conf.options.queue_model, self.QueueModel)

        with self.assertSystemExit(in_stderr='Unable to import "queue_model"'):
            WorkerConfig(self.mkargs('--queue-model=foo.bar'))

    def test_error_model_argument(self):
        conf = WorkerConfig(self.mkargs('--error-model=tests.workers.WorkerConfigArgumentsTest.ErrorModel'))
        self.assertEqual(conf.options.error_model, self.ErrorModel)

        with self.assertSystemExit(in_stderr='Unable to import "error_model"'):
            WorkerConfig(self.mkargs('--error-model=foo.bar'))

    def test_worker_class_argument(self):
        conf = WorkerConfig(self.mkargs('--worker-class=tests.workers.WorkerConfigArgumentsTest.WorkerClass'))
        self.assertEqual(conf.options.worker_class, self.WorkerClass)

        with self.assertSystemExit(in_stderr='Unable to import "worker_class"'):
            WorkerConfig(self.mkargs('--worker-class=foo.bar'))

    def test_callback_argument(self):
        conf = WorkerConfig(self.mkargs('--callback=tests.workers.WorkerConfigArgumentsTest.callback'))
        self.assertEqual(conf.options.callback, self.callback)

        with self.assertSystemExit(in_stderr='Unable to import "callback"'):
            WorkerConfig(self.mkargs('--callback=foo.bar'))

        with self.assertSystemExit(in_stderr='The callback is not callable'):
            WorkerConfig(self.mkargs('--callback=tests.workers.WorkerConfigArgumentsTest.not_a_callback'))

    def test_logger_arguments(self):
        conf = WorkerConfig(self.mkargs('--logger-base-name=foo --logger-level=debug'))
        self.assertEqual(conf.options.logger_base_name, 'foo')
        self.assertEqual(conf.options.logger_level, logging.DEBUG)

        conf = WorkerConfig(self.mkargs('--logger-level=10'))
        self.assertEqual(conf.options.logger_level, logging.DEBUG)

        conf = WorkerConfig(self.mkargs('--logger-level=15'))
        self.assertEqual(conf.options.logger_level, 15)

        with self.assertSystemExit(in_stderr='Invalid logger-level bar'):
            WorkerConfig(self.mkargs('--logger-level=bar'))

    def test_save_errors_arguments(self):
        conf = WorkerConfig(self.mkargs())
        self.assertIsNone(conf.options.save_errors)

        conf = WorkerConfig(self.mkargs('--save-errors'))
        self.assertTrue(conf.options.save_errors)

        conf = WorkerConfig(self.mkargs('--no-save-errors'))
        self.assertFalse(conf.options.save_errors)

    def test_max_loops_argument(self):
        conf = WorkerConfig(self.mkargs('--max-loops=100'))
        self.assertEqual(conf.options.max_loops, 100)

        with self.assertSystemExit(in_stderr='option --max-loops: invalid integer value:'):
            WorkerConfig(self.mkargs('--max-loops=foo'))

        with self.assertSystemExit(in_stderr='The max-loops argument'):
            WorkerConfig(self.mkargs('--max-loops=-1'))

    def test_terminate_gracefuly_arguments(self):
        conf = WorkerConfig(self.mkargs())
        self.assertIsNone(conf.options.terminate_gracefuly)

        conf = WorkerConfig(self.mkargs('--terminate-gracefuly'))
        self.assertTrue(conf.options.terminate_gracefuly)

        conf = WorkerConfig(self.mkargs('--no-terminate-gracefuly'))
        self.assertFalse(conf.options.terminate_gracefuly)

    def test_timeout_argument(self):
        conf = WorkerConfig(self.mkargs('--timeout=5'))
        self.assertEqual(conf.options.timeout, 5)

        with self.assertSystemExit(in_stderr="option --timeout: invalid integer value: 'none'"):
            WorkerConfig(self.mkargs('--timeout=none'))

        with self.assertSystemExit(in_stderr="must be a positive integer"):
            WorkerConfig(self.mkargs('--timeout=-1'))

    def test_database_argument(self):
        conf = WorkerConfig(self.mkargs('--database=localhost:6379:15'))
        self.assertEqual(conf.database_config, dict(host='localhost', port=6379, db=15))

    def test_title_argument(self):
        conf = WorkerConfig(self.mkargs())
        self.assertTrue(conf.update_title)

        conf = WorkerConfig(self.mkargs('--no-title'))
        self.assertFalse(conf.update_title)


# can't be defined in WorkerConfigRunTest and used in its *ModelOtherDB classes
other_database = PipelineDatabase(host='localhost', port=6379, db=15)


class WorkerConfigRunTest(WorkerConfigBaseTest):
    class WorkerClass(Worker):
        pass

    class JobModelOtherDB(Job):
        namespace = 'WorkerConfigRunTest'
        database = other_database

    class QueueModelOtherDB(Queue):
        namespace = 'WorkerConfigRunTest'
        database = other_database

    class ErrorModelOtherDB(Error):
        namespace = 'WorkerConfigRunTest'
        database = other_database

    def test_prepare_worker(self):
        conf = WorkerConfig(self.mkargs('--name=foo'))
        self.assertIsNone(getattr(conf, 'worker', None))

        conf.prepare_worker()
        self.assertIsInstance(conf.worker, Worker)

        conf = WorkerConfig(self.mkargs('--name=bar --worker-class=tests.workers.WorkerConfigRunTest.WorkerClass'))
        conf.prepare_worker()
        self.assertIsInstance(conf.worker, WorkerConfigRunTest.WorkerClass)
        self.assertFalse(conf.worker.end_forced)

        conf = WorkerConfig(self.mkargs('--name=baz --dry-run'))
        conf.prepare_worker()
        self.assertTrue(conf.worker.end_forced)

    def test_proc_title(self):
        conf = WorkerConfig(self.mkargs('--name=foo'))
        self.assertEqual('test-script [init]', conf.get_proc_title())

        conf.prepare_worker()
        self.assertEqual('test-script [init] queue=foo', conf.get_proc_title())

        conf.worker.status = 'waiting'
        self.assertEqual('test-script [waiting] queue=foo loop=0/1000 waiting-jobs=0', conf.get_proc_title())

        conf.worker.end_forced = True
        self.assertEqual('test-script [waiting - ending] queue=foo loop=0/1000 waiting-jobs=0', conf.get_proc_title())

    def test_prepare_models(self):
        conf = WorkerConfig(self.mkargs('--name=foo --database=localhost:6379:13'
                                        ' --job-model=tests.workers.WorkerConfigRunTest.JobModelOtherDB'
                                        ' --queue-model=tests.workers.WorkerConfigRunTest.QueueModelOtherDB'
                                        ' --error-model=tests.workers.WorkerConfigRunTest.ErrorModelOtherDB'))
        conf.prepare_models()
        for model_name in ('job', 'queue', 'error'):
            model = getattr(conf.options, '%s_model' % model_name)
            self.assertEqual(model.database.connection_settings['db'], 13)
