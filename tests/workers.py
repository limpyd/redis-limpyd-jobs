import logging
import threading
import time
import signal

from limpyd import fields
from limpyd.exceptions import ImplementationError, DoesNotExist

from limpyd_jobs.models import Queue, Job, Error
from limpyd_jobs.workers import Worker
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
                    save_errors=False
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

        worker = Worker(
                    name='testfoo',
                    queue_model=OtherTestQueue,
                    job_model=OtherTestJob,
                    error_model=OtherTestError,
                    logger_base_name='limpyd-jobs.workerfoo.%s',
                    logger_level=logging.INFO,
                    max_loops=200,
                    terminate_gracefuly=True,
                    save_errors=True
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
        worker.end_signal_catched = True
        self.assertEqual(worker.must_stop(), True)
        worker.terminate_gracefuly = False
        self.assertEqual(worker.must_stop(), False)

    def test_wait_for_job_should_respond_with_queue_and_job_when_a_queue_is_not_empty(self):
        queue = Queue.get_queue('test')
        worker = Worker('test')
        worker.test_content = None

        class Thread(threading.Thread):
            def run(self):
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
        self.assertEqual(worker.test_content[0].get_pk(), queue.get_pk())
        self.assertTrue(isinstance(worker.test_content[0], Queue))
        self.assertEqual(worker.test_content[1].get_pk(), job.get_pk())
        self.assertTrue(isinstance(worker.test_content[1], Job))

        # job must no be in the queue anymore
        self.assertFalse(job.get_pk() in queue.waiting.lmembers())

    def test_get_job_method_should_return_a_job_based_on_its_pk(self):
        worker = Worker('test')
        job = Job.add_job(queue_name='test', identifier='job:1')

        # a job...
        test_job = worker.get_job(job.get_pk())
        self.assertTrue(isinstance(test_job, Job))
        self.assertEqual(test_job.get_pk(), job.get_pk())

        # not a job...
        with self.assertRaises(ValueError):
            worker.get_job('foo')

    def test_get_queue_method_should_return_a_queue_based_on_its_waiting_field_key(self):
        worker = Worker('test')
        queue = Queue.get_queue('test')

        # a queue...
        test_queue = worker.get_queue(queue.waiting.key)
        self.assertTrue(isinstance(test_queue, Queue))
        self.assertEqual(test_queue.get_pk(), queue.get_pk())

        # a non_existing_queue
        fake_queue_key = test_queue.waiting.key.replace('1', '2')
        with self.assertRaises(ValueError):
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
            result['job'] = job.get_pk()
            result['queue'] = queue.get_pk()

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', callback=callback, max_loops=1)
        worker.run()

        self.assertEqual(result, {'job': job.get_pk(), 'queue': queue.get_pk()})

    def test_job_success_method_should_be_called(self):
        def callback(job, queue):
            pass

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', callback=callback, max_loops=1)
        worker.run()

        self.assertEqual(job.status.hget(), STATUSES.SUCCESS)
        self.assertTrue(job.get_pk() in queue.success.lmembers())

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
        self.assertTrue(job1.get_pk() in queue.errors.lmembers())
        self.assertEqual(len(Error.collection()), 1)
        error = Error.get(identifier='job:1')
        self.assertEqual(error.message.hget(), 'You must implement your own action')
        self.assertEqual(error.code.hget(), None)

        job2 = Job.add_job(identifier='job:2', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = Worker(name='test', max_loops=1, callback=callback)  # callback with exception
        worker.run()

        self.assertEqual(job2.status.hget(), STATUSES.ERROR)
        self.assertTrue(job2.get_pk() in queue.errors.lmembers())
        self.assertEqual(len(Error.collection()), 2)
        error = Error.get(identifier='job:2')
        self.assertEqual(error.message.hget(), 'foobar')
        self.assertEqual(error.code.hget(), '42')

    def test_error_model_with_additional_fields(self):
        class TestError(Error):
            foo = fields.HashableField()

        class TestWorker(Worker):
            error_model = TestError

            def additional_error_fields(self, job, queue, exception):
                return {
                    'foo': 'Error on queue %s for job %s' % (
                        job.get_pk(), queue.get_pk()
                    )
                }

        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue(name='test')
        worker = TestWorker(name='test', max_loops=1)  # no callback
        worker.run()

        error = TestError.get(identifier='job:1')
        attended_foo = 'Error on queue %s for job %s' % (job.get_pk(), queue.get_pk())
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

        self.assertEqual(worker.end_signal_catched, True)
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
