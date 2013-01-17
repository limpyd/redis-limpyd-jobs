from datetime import datetime, timedelta

from limpyd import fields

from limpyd_jobs.models import Queue, Job, Error
from limpyd_jobs import STATUSES

from .base import LimpydBaseTest


class QueuesTest(LimpydBaseTest):

    def count_queues(self):
        return len(Queue.collection())

    def test_non_existing_queue_could_be_created(self):
        count_before = self.count_queues()
        Queue.get_queue(name='test')
        count_after = self.count_queues()
        self.assertEqual(count_after, count_before + 1)

    def test_existing_queue_should_be_returned(self):
        # create one
        Queue.get_queue(name='test')
        # and get it
        count_before = self.count_queues()
        Queue.get_queue(name='test')
        count_after = self.count_queues()
        self.assertEqual(count_after, count_before)

    def test_get_keys_should_return_all_keys_for_a_name(self):
        # create two with the same name and different priorities
        q0 = Queue.get_queue(name='test', priority=0)
        q1 = Queue.get_queue(name='test', priority=1)
        # and one with a different name
        qx = Queue.get_queue(name='foobar')

        # test we can get all keys for 'test', ordered by priority desc
        keys = Queue.get_keys('test')
        self.assertEqual(keys, [q1.waiting.key, q0.waiting.key])

        # tests for foobar
        keys = Queue.get_keys('foobar')
        self.assertEqual(keys, [qx.waiting.key])

        # tests for non existing name
        keys = Queue.get_keys('qux')
        self.assertEqual(keys, [])

    def test_extended_queue_can_accept_other_fields(self):
        class ExtendedQueue(Queue):
            namespace = 'test-queuestest'
            foo = fields.StringField()
            bar = fields.StringField()

        # create a new queue
        queue = ExtendedQueue.get_queue(name='test', priority=1, foo='FOO', bar='BAR')
        self.assertEqual(queue.foo.get(), 'FOO')
        self.assertEqual(queue.bar.get(), 'BAR')

        # get the same queue, extended fields won't be updated
        queue = ExtendedQueue.get_queue(name='test', priority=1, foo='FOO2', bar='BAR2')
        self.assertEqual(queue.foo.get(), 'FOO')
        self.assertEqual(queue.bar.get(), 'BAR')


class JobsTests(LimpydBaseTest):

    def assert_job_status_and_priority(self, job, status, priority):
        job_status, job_priority = job.hmget('status', 'priority')
        self.assertEqual(job_status, status)
        self.assertEqual(job_priority, priority)

    def test_adding_a_job_should_create_a_queue_with_the_job(self):
        job = Job.add_job(identifier='job:1', queue_name='test', priority=5)

        # count queues
        keys = Queue.get_keys('test')
        self.assertEqual(len(keys), 1)

        # get the new queue, should not create it (number of keys should be 1)
        queue = Queue.get_queue(name='test', priority=5)
        keys = Queue.get_keys('test')
        self.assertEqual(len(keys), 1)

        # check that the job is in the queue
        jobs = queue.waiting.lrange(0, -1)
        self.assertEqual(jobs, [str(job.pk.get())])

        # ... with the correct status and priority
        self.assert_job_status_and_priority(job, STATUSES.WAITING, '5')

    def test_adding_an_existing_job_should_do_nothing(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=3)
        job2 = Job.add_job(identifier='job:1', queue_name='test', priority=3)

        # only one job
        self.assertEqual(job1.pk.get(), job2.pk.get())
        # is in high priority queue
        queue = Queue.get_queue(name='test', priority=3)
        self.assertEqual(queue.waiting.llen(), 1)
        # nothing in high priority queue
        queue = Queue.get_queue(name='test', priority=1)
        self.assertEqual(queue.waiting.llen(), 0)

        # we should still have original priority and status
        self.assert_job_status_and_priority(job1, STATUSES.WAITING, '3')
        # idem for job2 (which is, in fact, job1)
        self.assert_job_status_and_priority(job2, STATUSES.WAITING, '3')

    def test_adding_an_existing_job_with_lower_priority_should_do_nothing(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=3)
        job2 = Job.add_job(identifier='job:1', queue_name='test', priority=1)

        # only one job
        self.assertEqual(job1.pk.get(), job2.pk.get())
        # is in high priority queue
        queue = Queue.get_queue(name='test', priority=3)
        self.assertEqual(queue.waiting.llen(), 1)
        # nothing in high priority queue
        queue = Queue.get_queue(name='test', priority=1)
        self.assertEqual(queue.waiting.llen(), 0)

        # we should still have original priority and status
        self.assert_job_status_and_priority(job1, STATUSES.WAITING, '3')
        # idem for job2 (which is, in fact, job1)
        self.assert_job_status_and_priority(job2, STATUSES.WAITING, '3')

    def test_adding_an_existing_job_with_higher_priority_should_change_its_queue(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        queue1 = Queue.get_queue(name='test', priority=1)
        job2 = Job.add_job(identifier='job:1', queue_name='test', priority=2)
        queue2 = Queue.get_queue(name='test', priority=2)

        # only one job
        self.assertEqual(job1.pk.get(), job2.pk.get())
        # not anymore in queue with priority 1
        self.assertEqual(queue1.waiting.llen(), 0)
        # but now in queue with priority 2
        self.assertEqual(queue2.waiting.llen(), 1)

        # the new prioriy must be stored, and status should still be waiting
        self.assert_job_status_and_priority(job1, STATUSES.WAITING, '2')
        # idem for job2 (which is, in fact, job1)
        self.assert_job_status_and_priority(job2, STATUSES.WAITING, '2')

    def test_prepending_an_existing_job_should_move_it_at_the_beginning(self):
        queue = Queue.get_queue(name='test', priority=1)

        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        job2 = Job.add_job(identifier='job:2', queue_name='test', priority=1)
        self.assertEqual(queue.waiting.lmembers(), [job1.pk.get(), job2.pk.get()])

        Job.add_job(identifier='job:2', queue_name='test', priority=1, prepend=True)
        self.assertEqual(queue.waiting.lmembers(), [job2.pk.get(), job1.pk.get()])

    def test_prepending_a_new_job_should_add_it_at_the_beginning(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        job2 = Job.add_job(identifier='job:2', queue_name='test', priority=1, prepend=True)
        queue = Queue.get_queue(name='test', priority=1)
        self.assertEqual(queue.waiting.lmembers(), [job2.pk.get(), job1.pk.get()])

    def test_duration_should_compute_end_start_difference(self):
        start = datetime.utcnow()
        job = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        job.hmset(start=start, end=start + timedelta(seconds=2))
        duration = job.duration
        self.assertEqual(duration, timedelta(seconds=2))

    def test_extended_job_can_accept_other_fields(self):
        class ExtendedJob(Job):
            namespace = 'test-jobstest'
            foo = fields.StringField()
            bar = fields.StringField()

        # create a new job
        job = ExtendedJob.add_job(identifier='job:1', queue_name='test',
                                    priority=1, foo='FOO', bar='BAR')
        self.assertEqual(job.foo.get(), 'FOO')
        self.assertEqual(job.bar.get(), 'BAR')

        # get the same job, extended fields won't be updated
        job = ExtendedJob.add_job(identifier='job:1', queue_name='test',
                                    priority=1, foo='FOO2', bar='BAR2')
        self.assertEqual(job.foo.get(), 'FOO')
        self.assertEqual(job.bar.get(), 'BAR')

    def test_using_a_subclass_of_queue_should_work(self):
        class TestQueue(Queue):
            namespace = 'test_using_a_subclass_of_queue_should_work'

        class TestJob(Job):
            namespace = 'test_using_a_subclass_of_queue_should_work'

        # not specifying queue_model should use the default Queue model
        default_queue = Queue.get_queue('test1')
        queue = TestQueue.get_queue('test1')
        Job.add_job(identifier='job:1', queue_name='test1')
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(default_queue.waiting.llen(), 1)

        # idem with a subclass of job
        default_queue = Queue.get_queue('test2')
        queue = TestQueue.get_queue('test2')
        TestJob.add_job(identifier='job:2', queue_name='test2')
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(default_queue.waiting.llen(), 1)

        # specifiying a queue_model in add_job should use the wanted queue
        default_queue = Queue.get_queue('test3')
        queue = TestQueue.get_queue('test3')
        Job.add_job(identifier='job:3', queue_name='test3', queue_model=TestQueue)
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(default_queue.waiting.llen(), 0)

        # idem with a subclass of job
        default_queue = Queue.get_queue('test4')
        queue = TestQueue.get_queue('test4')
        TestJob.add_job(identifier='job:4', queue_name='test4', queue_model=TestQueue)
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(default_queue.waiting.llen(), 0)

        # now test with a queue_model defined in the job class
        class TestJobWithQueueModel(Job):
            namespace = 'test_using_a_subclass_of_queue_should_work'
            queue_model = TestQueue

        default_queue = Queue.get_queue('test5')
        queue = TestQueue.get_queue('test5')
        TestJobWithQueueModel.add_job(identifier='job:5', queue_name='test5')
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(default_queue.waiting.llen(), 0)


class ErrorsTest(LimpydBaseTest):

    class ExceptionWithCode(Exception):
        def __init__(self, message, code):
            super(ErrorsTest.ExceptionWithCode, self).__init__(message)
            self.message = message
            self.code = code

    def test_add_error_method_should_add_an_error_instance(self):
        e = ErrorsTest.ExceptionWithCode('the answer', 42)
        error1 = Error.add_error(queue_name='test', identifier='job:1', error=e)
        self.assertEqual(list(Error.collection()), [error1.pk.get()])
        Error.add_error(queue_name='test', identifier='job:1', error=e)
        self.assertEqual(len(Error.collection()), 2)

    def test_add_error_can_accept_an_exception_without_code(self):
        e = Exception('no code')
        error = Error.add_error(queue_name='test', identifier='job:1', error=e)
        self.assertEqual(error.code.hget(), None)

    def test_add_error_should_store_the_name_of_the_exception(self):
        e = Exception('foo')
        error = Error.add_error(queue_name='test', identifier='job:1', error=e)
        self.assertEqual(error.type.hget(), 'Exception')

        e = ErrorsTest.ExceptionWithCode('the answer', 42)
        error = Error.add_error(queue_name='test', identifier='job:1', error=e)
        self.assertEqual(error.type.hget(), 'ExceptionWithCode')

    def test_new_error_save_date_and_time_appart(self):
        e = ErrorsTest.ExceptionWithCode('the answer', 42)
        day = datetime(2012, 9, 29, 22, 58, 56)
        error = Error.add_error(queue_name='test', identifier='job:1', error=e,
                                                                    when=day)
        self.assertEqual(error.date.hget(), '2012-09-29')
        self.assertEqual(error.time.hget(), '22:58:56')
        self.assertEqual(error.datetime, day)

    def test_extended_error_can_accept_other_fields(self):
        class ExtendedError(Error):
            namespace = 'test-errorstest'
            foo = fields.StringField()
            bar = fields.StringField()

        e = ErrorsTest.ExceptionWithCode('the answer', 42)

        # create a new error
        error = ExtendedError.add_error(queue_name='test', identifier='job:1',
                                        error=e, foo='FOO', bar='BAR')
        self.assertEqual(error.foo.get(), 'FOO')
        self.assertEqual(error.bar.get(), 'BAR')
