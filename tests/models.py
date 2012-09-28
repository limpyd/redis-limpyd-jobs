from datetime import datetime, timedelta

from limpyd_jobs.models import Queue, Job
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
        self.assertEqual(jobs, [str(job.get_pk())])

        # ... with the correct status and priority
        self.assert_job_status_and_priority(job, STATUSES.WAITING, '5')

    def test_adding_an_existing_job_should_do_nothing(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=3)
        job2 = Job.add_job(identifier='job:1', queue_name='test', priority=3)

        # only one job
        self.assertEqual(job1.get_pk(), job2.get_pk())
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
        self.assertEqual(job1.get_pk(), job2.get_pk())
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
        self.assertEqual(job1.get_pk(), job2.get_pk())
        # not anymore in queue with priority 1
        self.assertEqual(queue1.waiting.llen(), 0)
        # but now in queue with priority 2
        self.assertEqual(queue2.waiting.llen(), 1)

        # the new prioriy must be stored, and status should still be waiting
        self.assert_job_status_and_priority(job1, STATUSES.WAITING, '2')
        # idem for job2 (which is, in fact, job1)
        self.assert_job_status_and_priority(job2, STATUSES.WAITING, '2')

    def test_duration_should_compute_end_start_difference(self):
        start = datetime.utcnow()
        job = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        job.hmset(start=start, end=start + timedelta(seconds=2))
        duration = job.duration
        self.assertEqual(duration, timedelta(seconds=2))
