from datetime import datetime, timedelta
from dateutil.parser import parse
from time import sleep
import traceback

from limpyd import fields

from limpyd_jobs.models import Queue, Job, Error, datetime_to_score
from limpyd_jobs import STATUSES, LimpydJobsException

from .base import LimpydBaseTest


class QueueTests(LimpydBaseTest):

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

    def test_get_waiting_keys_should_return_all_keys_for_a_name(self):
        # create two with the same name and different priorities
        q0 = Queue.get_queue(name='test', priority=0)
        q1 = Queue.get_queue(name='test', priority=1)
        # and one with a different name
        qx = Queue.get_queue(name='foobar')

        # test we can get all keys for 'test', ordered by priority desc
        keys = Queue.get_waiting_keys('test')
        self.assertEqual(keys, [q1.waiting.key, q0.waiting.key])

        # tests for foobar
        keys = Queue.get_waiting_keys('foobar')
        self.assertEqual(keys, [qx.waiting.key])

        # tests for non existing name
        keys = Queue.get_waiting_keys('qux')
        self.assertEqual(keys, [])

    def test_get_all_should_return_all_queues(self):
        qa0 = Queue.get_queue(name='testa', priority=0)
        qa10 = Queue.get_queue(name='testa', priority=10)
        qa5 = Queue.get_queue(name='testa', priority=5)
        qb1 = Queue.get_queue(name='testb', priority=1)
        qb7 = Queue.get_queue(name='testb', priority=7)
        qb15 = Queue.get_queue(name='testb', priority=15)

        queuesa = set([q.pk.get() for q in Queue.get_all('testa')])
        self.assertEqual(queuesa, set([qa0.pk.get(), qa5.pk.get(), qa10.pk.get()]))
        queuesb = set([q.pk.get() for q in Queue.get_all('testb')])
        self.assertEqual(queuesb, set([qb1.pk.get(), qb7.pk.get(), qb15.pk.get()]))

        queues = set([q.pk.get() for q in Queue.get_all(['testa', 'testb'])])
        self.assertEqual(queues, set([qa0.pk.get(), qa5.pk.get(), qa10.pk.get(),
                                      qb1.pk.get(), qb7.pk.get(), qb15.pk.get()]))

    def test_get_all_by_priority_should_return_all_queues_sorted(self):
        qa0 = Queue.get_queue(name='testa', priority=0)
        qa10 = Queue.get_queue(name='testa', priority=10)
        qa5 = Queue.get_queue(name='testa', priority=5)
        qb1 = Queue.get_queue(name='testb', priority=1)
        qb7 = Queue.get_queue(name='testb', priority=7)
        qb15 = Queue.get_queue(name='testb', priority=15)

        queuesa = [q.pk.get() for q in Queue.get_all_by_priority('testa')]
        self.assertEqual(queuesa, [qa10.pk.get(), qa5.pk.get(), qa0.pk.get()])
        queuesb = [q.pk.get() for q in Queue.get_all_by_priority('testb')]
        self.assertEqual(queuesb, [qb15.pk.get(), qb7.pk.get(), qb1.pk.get()])

        queues = [q.pk.get() for q in Queue.get_all_by_priority(['testa', 'testb'])]
        self.assertEqual(queues, [qb15.pk.get(), qa10.pk.get(), qb7.pk.get(),
                                      qa5.pk.get(), qb1.pk.get(), qa0.pk.get()])

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

    def test_count_waiting_jobs_should_return_the_number_of_waiting_jobs(self):
        self.assertEqual(Queue.count_waiting_jobs('test'), 0)
        Job.add_job(identifier='job:1', queue_name='test')
        self.assertEqual(Queue.count_waiting_jobs('test'), 1)
        Job.add_job(identifier='job:2', queue_name='test', delayed_for=5)
        self.assertEqual(Queue.count_waiting_jobs('test'), 1)
        Job.add_job(identifier='foo:1', queue_name='foo')
        Job.add_job(identifier='bar:1', queue_name='bar')
        self.assertEqual(Queue.count_waiting_jobs(['foo', 'bar']), 2)
        self.assertEqual(Queue.count_waiting_jobs(['test', 'foo', 'bar']), 3)

    def test_count_delayed_jobs_should_return_the_number_of_delayed_jobs(self):
        self.assertEqual(Queue.count_delayed_jobs('test'), 0)
        Job.add_job(identifier='job:1', queue_name='test', delayed_for=5)
        self.assertEqual(Queue.count_delayed_jobs('test'), 1)
        Job.add_job(identifier='job:2', queue_name='test')
        self.assertEqual(Queue.count_delayed_jobs('test'), 1)
        Job.add_job(identifier='foo:1', queue_name='foo', delayed_for=5)
        Job.add_job(identifier='bar:1', queue_name='bar', delayed_for=5)
        self.assertEqual(Queue.count_delayed_jobs(['foo', 'bar']), 2)
        self.assertEqual(Queue.count_delayed_jobs(['test', 'foo', 'bar']), 3)

    def test_first_delayed_should_return_the_first_delayed_job_to_be_ready(self):
        Job.add_job(identifier='job:1', queue_name='test', delayed_for=5)
        job2 = Job.add_job(identifier='job:2', queue_name='test', delayed_for=1)

        queue = Queue.get_queue(name='test')
        attended_timestamp = datetime_to_score(parse(job2.delayed_until.hget()))

        job_ident, timestamp = queue.first_delayed
        self.assertEqual(job_ident, job2.ident)
        self.assertEqual(timestamp, attended_timestamp)

        timestamp = queue.first_delayed_time
        self.assertEqual(timestamp, attended_timestamp)

    def test_delay_job_should_add_the_job_to_the_delayed_sortedset(self):
        delayed_until = datetime.utcnow() + timedelta(seconds=1)
        timestamp = datetime_to_score(delayed_until)

        job = Job(identifier='job:1', delayed_until=str(delayed_until))

        queue = Queue.get_queue(name='test')
        queue.delay_job(job, delayed_until)

        delayed_jobs = queue.delayed.zrange(0, -1, withscores=True)
        self.assertEqual(delayed_jobs, [(job.ident, timestamp)])

        # test a non-delayed job
        job2 = Job(identifier='job:2')
        queue.enqueue_job(job2)

        delayed_jobs = queue.delayed.zrange(0, -1, withscores=True)
        self.assertEqual(delayed_jobs, [(job.ident, timestamp)])

    def test_enqueue_job_should_add_the_job_to_the_waiting_list(self):
        job = Job(identifier='job:1')

        queue = Queue.get_queue(name='test')
        queue.enqueue_job(job)

        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job.ident])

        # test a delayed job
        delayed_until = datetime.utcnow() + timedelta(seconds=1)
        job2 = Job(identifier='job:2', delayed_until=str(delayed_until))
        queue.delay_job(job2, delayed_until)

        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job.ident])

    def test_enqueue_job_can_prepend_jobs(self):
        queue = Queue.get_queue(name='test')

        job = Job(identifier='job:1')
        queue.enqueue_job(job)
        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job.ident])

        job2 = Job(identifier='job:2')
        queue.enqueue_job(job2)
        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job.ident, job2.ident])

        job3 = Job(identifier='job:3')
        queue.enqueue_job(job3, prepend=True)
        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job3.ident, job.ident, job2.ident])

    def test_requeue_delayed_jobs_put_back_ready_delayed_jobs_to_the_waiting_list(self):
        queue = Queue.get_queue(name='test')

        job1 = Job.add_job(identifier='job:1', queue_name='test', delayed_for=10)
        job2 = Job.add_job(identifier='job:2', queue_name='test', delayed_for=1)

        self.assertEqual(Queue.count_waiting_jobs('test'), 0)
        self.assertEqual(Queue.count_delayed_jobs('test'), 2)

        self.assertEqual(job1.status.hget(), STATUSES.DELAYED)
        self.assertEqual(job2.status.hget(), STATUSES.DELAYED)

        # must not move any jobs, too soon
        queue.requeue_delayed_jobs()
        self.assertEqual(Queue.count_waiting_jobs('test'), 0)
        self.assertEqual(Queue.count_delayed_jobs('test'), 2)

        self.assertEqual(job1.status.hget(), STATUSES.DELAYED)
        self.assertEqual(job2.status.hget(), STATUSES.DELAYED)

        sleep(1)

        # now we should have one job in the waiting list
        queue.requeue_delayed_jobs()
        self.assertEqual(Queue.count_waiting_jobs('test'), 1)
        self.assertEqual(Queue.count_delayed_jobs('test'), 1)

        self.assertEqual(job1.status.hget(), STATUSES.DELAYED)
        self.assertEqual(job2.status.hget(), STATUSES.WAITING)

        waiting_jobs = queue.waiting.lmembers()
        self.assertEqual(waiting_jobs, [job2.ident])
        delayed_jobs = queue.delayed.zrange(0, -1, withscores=True)
        self.assertEqual(delayed_jobs[0][0], job1.ident)

    def test_requeue_delayed_jobs_should_abort_if_another_thread_works_on_it(self):
        queue = Queue.get_queue(name='test')

        # simulate a lock on this queue
        lock_key = queue.make_key(
            queue._name,
            queue.pk.get(),
            "requeue_all_delayed_ready_jobs",
        )

        queue.get_connection().set(lock_key, 1)

        Job.add_job(identifier='job:1', queue_name='test', delayed_for=0.1)
        self.assertEqual(Queue.count_waiting_jobs('test'), 0)
        self.assertEqual(Queue.count_delayed_jobs('test'), 1)

        # wait until the job should be ready
        sleep(0.5)

        queue.requeue_delayed_jobs()

        # the requeue must have done nothing
        self.assertEqual(Queue.count_waiting_jobs('test'), 0)
        self.assertEqual(Queue.count_delayed_jobs('test'), 1)


class JobFooBar(Job):
    pass


class JobTests(LimpydBaseTest):

    def assert_job_status_and_priority(self, job, status, priority):
        job_status, job_priority = job.hmget('status', 'priority')
        self.assertEqual(job_status, status)
        self.assertEqual(job_priority, priority)

    def test_get_model_repr_should_return_the_string_repr_of_the_model(self):
        self.assertEqual(Job.get_model_repr(), 'limpyd_jobs.models.Job')
        job = Job()
        self.assertEqual(job.get_model_repr(), 'limpyd_jobs.models.Job')

        self.assertEqual(JobFooBar.get_model_repr(), 'tests.models.JobFooBar')
        job = JobFooBar()
        self.assertEqual(job.get_model_repr(), 'tests.models.JobFooBar')

    def test_ident_should_return_a_string_repr_of_the_job(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        self.assertEqual(job.ident, 'limpyd_jobs.models.Job:%s' % job.pk.get())

        job = JobFooBar.add_job(identifier='foobar:1', queue_name='foobar')
        self.assertEqual(job.ident, 'tests.models.JobFooBar:%s' % job.pk.get())

    def test_get_from_ident_should_return_a_job(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        test_job = Job.get_from_ident('limpyd_jobs.models.Job:%s' % job.pk.get())
        self.assertEqual(job.ident, test_job.ident)

        job = JobFooBar.add_job(identifier='foobar:1', queue_name='foobar')
        test_job = Job.get_from_ident('tests.models.JobFooBar:%s' % job.pk.get())
        self.assertEqual(job.ident, test_job.ident)

    def test_adding_a_job_should_create_a_queue_with_the_job(self):
        job = Job.add_job(identifier='job:1', queue_name='test', priority=5)

        # count queues
        keys = Queue.get_waiting_keys('test')
        self.assertEqual(len(keys), 1)

        # get the new queue, should not create it (number of keys should be 1)
        queue = Queue.get_queue(name='test', priority=5)
        keys = Queue.get_waiting_keys('test')
        self.assertEqual(len(keys), 1)

        # check that the job is in the queue
        jobs = queue.waiting.lrange(0, -1)
        self.assertEqual(jobs, [job.ident])

        # ... with the correct status and priority
        self.assert_job_status_and_priority(job, STATUSES.WAITING, '5')

    def test_invalid_type_for_delayed_for_argument_of_add_job_should_raise(self):
        with self.assertRaises(ValueError):
            Job.add_job(identifier='job:1', queue_name='test', delayed_for='foo')

    def test_adding_a_job_with_delayed_for_should_add_the_job_in_the_delayed_list(self):
        queue = Queue.get_queue(name='test')

        job = Job.add_job(identifier='job:1', queue_name='test', delayed_for=5)
        self.assertEqual(job.status.hget(), STATUSES.DELAYED)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 1)

        job2 = Job.add_job(identifier='job:2', queue_name='test', delayed_for=5.5)
        self.assertEqual(job2.status.hget(), STATUSES.DELAYED)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 2)

        job3 = Job.add_job(identifier='job:3', queue_name='test', delayed_for=timedelta(seconds=5))
        self.assertEqual(job3.status.hget(), STATUSES.DELAYED)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 3)

    def test_invalid_type_for_delayed_until_argument_of_add_job_should_raise(self):
        with self.assertRaises(ValueError):
            Job.add_job(identifier='job:1', queue_name='test', delayed_until='foo')

    def test_delayed_for_and_delayed_until_should_be_exclusive(self):
        with self.assertRaises(ValueError):
            Job.add_job(identifier='job:1', queue_name='test', delayed_for=5,
                        delayed_until=datetime.utcnow()+timedelta(seconds=5))

    def test_adding_a_job_with_delayed_until_in_the_future_should_add_the_job_in_the_delayed_list(self):
        queue = Queue.get_queue(name='test')

        job = Job.add_job(identifier='job:1', queue_name='test', delayed_until=datetime.utcnow() + timedelta(seconds=5))
        self.assertEqual(job.status.hget(), STATUSES.DELAYED)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 1)

    def test_adding_a_job_with_delayed_until_not_in_the_future_should_add_the_job_in_the_waiting_list(self):
        queue = Queue.get_queue(name='test')

        job = Job.add_job(identifier='job:1', queue_name='test', delayed_until=datetime.utcnow() - timedelta(seconds=5))
        self.assertEqual(job.status.hget(), STATUSES.WAITING)
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(queue.delayed.zcard(), 0)

    def test_adding_an_existing_job_should_do_nothing(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=3)
        job2 = Job.add_job(identifier='job:1', queue_name='test', priority=3)

        # only one job
        self.assertEqual(job1.ident, job2.ident)
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
        self.assertEqual(job1.ident, job2.ident)
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
        self.assertEqual(job1.ident, job2.ident)
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
        self.assertEqual(queue.waiting.lmembers(), [job1.ident, job2.ident])

        Job.add_job(identifier='job:2', queue_name='test', priority=1, prepend=True)
        self.assertEqual(queue.waiting.lmembers(), [job2.ident, job1.ident])

    def test_prepending_a_new_job_should_add_it_at_the_beginning(self):
        job1 = Job.add_job(identifier='job:1', queue_name='test', priority=1)
        job2 = Job.add_job(identifier='job:2', queue_name='test', priority=1, prepend=True)
        queue = Queue.get_queue(name='test', priority=1)
        self.assertEqual(queue.waiting.lmembers(), [job2.ident, job1.ident])

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

    def test_enqueue_or_delay_should_delay_if_in_the_future(self):
        queue = Queue.get_queue(name='test', priority=1)
        job = Job(identifier='job:1')
        delayed_until = datetime.utcnow() + timedelta(seconds=5)
        job.enqueue_or_delay('test', 1, delayed_until=delayed_until)
        self.assertEqual(queue.delayed.zcard(), 1)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(job.delayed_until.hget(), str(delayed_until))
        self.assertEqual(job.status.hget(), STATUSES.DELAYED)
        self.assertEqual(job.priority.hget(), '1')

    def test_enqueue_or_delay_should_enqueue_if_not_in_the_future(self):
        queue = Queue.get_queue(name='test', priority=1)

        job = Job(identifier='job:1')
        job.enqueue_or_delay('test', 1)
        self.assertEqual(queue.delayed.zcard(), 0)
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(job.delayed_until.hget(), None)
        self.assertEqual(job.status.hget(), STATUSES.WAITING)
        self.assertEqual(job.priority.hget(), '1')

        job2 = Job(identifier='job:2')
        delayed_until = datetime.utcnow() - timedelta(seconds=5)  # the past !
        job2.enqueue_or_delay('test', 1, delayed_until=delayed_until)
        self.assertEqual(queue.delayed.zcard(), 0)
        self.assertEqual(queue.waiting.llen(), 2)
        self.assertEqual(job2.delayed_until.hget(), None)
        self.assertEqual(job2.status.hget(), STATUSES.WAITING)
        self.assertEqual(job2.priority.hget(), '1')

    def test_duration_property_should_return_elapsed_time_during_start_and_end(self):
        job = Job(identifier='job:1')
        self.assertIsNone(job.duration)

        start = datetime.utcnow()
        job.start.hset(str(start))
        self.assertIsNone(job.duration)

        end = start + timedelta(seconds=5)
        job.end.hset(str(end))
        self.assertEqual(job.duration, timedelta(seconds=5))

        job.start.delete()
        self.assertIsNone(job.duration)

    def test_a_job_not_in_error_couldnt_be_requeued(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        with self.assertRaises(LimpydJobsException):
            job.requeue('test')

    def test_a_job_in_error_could_be_requeued(self):
        job = Job(identifier='job:1', status=STATUSES.ERROR)
        job.requeue('test')
        queue = Queue.get_queue('test')
        self.assertEqual(queue.waiting.llen(), 1)
        self.assertEqual(queue.delayed.zcard(), 0)
        self.assertEqual(job.status.hget(), STATUSES.WAITING)

    def test_requeuing_a_job_with_a_delay_delta_should_put_it_in_the_delayed_lits(self):
        job = Job(identifier='job:1', status=STATUSES.ERROR)
        queue = Queue.get_queue('test')
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 0)
        job.requeue('test', delayed_for=5)
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 1)
        self.assertEqual(job.status.hget(), STATUSES.DELAYED)

    def test_requeuing_a_job_with_a_datetime_should_put_it_in_the_delayed_lits(self):
        job = Job(identifier='job:1', status=STATUSES.ERROR)
        queue = Queue.get_queue('test')
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 0)
        job.requeue('test', delayed_until=datetime.utcnow()+timedelta(seconds=5))
        self.assertEqual(queue.waiting.llen(), 0)
        self.assertEqual(queue.delayed.zcard(), 1)
        self.assertEqual(job.status.hget(), STATUSES.DELAYED)

    def test_run_method_should_raise_by_default(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        queue = Queue.get_queue('test')
        with self.assertRaises(NotImplementedError):
            job.run(queue)

    def test_add_job_without_queue_name_should_use_the_class_one(self):
        class JobWithQueueName(Job):
            queue_name = 'test'

        JobWithQueueName.add_job(identifier='job:1')
        queue = Queue.get_queue('test')

        self.assertEqual(queue.waiting.llen(), 1)

    def test_add_job_without_queue_name_should_fail_if_no_class_one(self):
        with self.assertRaises(LimpydJobsException):
            Job.add_job(identifier='job:1')

    def test_requeue_without_queue_name_should_use_the_class_one(self):
        class JobWithQueueName2(Job):
            queue_name = 'test'

        job = JobWithQueueName2(identifier='job:1', status=STATUSES.ERROR)
        job.requeue(delayed_for=5)

        queue = Queue.get_queue('test')
        self.assertEqual(queue.delayed.zcard(), 1)

    def test_requeue_without_queue_name_should_fail_if_no_class_one(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        with self.assertRaises(LimpydJobsException):
            job.requeue()

    def test_enqueue_or_delay_without_queue_name_should_use_the_class_one(self):
        class JobWithQueueName3(Job):
            queue_name = 'test'

        job = JobWithQueueName3(identifier='job:1')
        job.enqueue_or_delay(delayed_until=datetime.utcnow()+timedelta(seconds=5))

        queue = Queue.get_queue('test')
        self.assertEqual(queue.delayed.zcard(), 1)

    def test_enqueue_or_delay_without_queue_name_should_fail_if_no_class_one(self):
        job = Job.add_job(identifier='job:1', queue_name='test')
        with self.assertRaises(LimpydJobsException):
            job.enqueue_or_delay(delayed_until=datetime.utcnow()+timedelta(seconds=5))


class ErrorTests(LimpydBaseTest):

    class ExceptionWithCode(Exception):
        def __init__(self, message, code):
            super(ErrorTests.ExceptionWithCode, self).__init__(message)
            self.message = message
            self.code = code

    def test_add_error_method_should_add_an_error_instance(self):
        e = ErrorTests.ExceptionWithCode('the answer', 42)
        job = Job.add_job(identifier='job:1', queue_name='test')
        error1 = Error.add_error(queue_name='test', job=job, error=e)
        self.assertEqual(list(Error.collection()), [error1.pk.get()])
        Error.add_error(queue_name='test', job=job, error=e)
        self.assertEqual(len(Error.collection()), 2)

    def test_add_error_can_accept_an_exception_without_code(self):
        e = Exception('no code')
        job = Job.add_job(identifier='job:1', queue_name='test')
        error = Error.add_error(queue_name='test', job=job, error=e)
        self.assertEqual(error.code.hget(), None)

    def test_add_error_should_store_the_name_of_the_exception(self):
        e = Exception('foo')
        job = Job.add_job(identifier='job:1', queue_name='test')
        error = Error.add_error(queue_name='test', job=job, error=e)
        self.assertEqual(error.type.hget(), 'Exception')

        e = ErrorTests.ExceptionWithCode('the answer', 42)
        error = Error.add_error(queue_name='test', job=job, error=e)
        self.assertEqual(error.type.hget(), 'ExceptionWithCode')

    def test_new_error_save_job_pk_and_identifier_appart(self):
        e = ErrorTests.ExceptionWithCode('the answer', 42)
        when = datetime(2012, 9, 29, 22, 58, 56)
        job = Job.add_job(identifier='job:1', queue_name='test')
        error = Error.add_error(queue_name='test', job=job, error=e, when=when)
        self.assertEqual(error.job_model_repr.hget(), job.get_model_repr())
        self.assertEqual(error.job_pk.hget(), job.pk.get())
        self.assertEqual(error.identifier.hget(), 'job:1')
        self.assertEqual(list(Error.collection(job_pk=job.pk.get())), [error.pk.get()])
        self.assertEqual(list(Error.collection(identifier=job.identifier.hget())), [error.pk.get()])

    def test_new_error_save_date_and_time_appart(self):
        e = ErrorTests.ExceptionWithCode('the answer', 42)
        when = datetime(2012, 9, 29, 22, 58, 56)
        job = Job.add_job(identifier='job:1', queue_name='test')
        error = Error.add_error(queue_name='test', job=job, error=e, when=when)
        self.assertEqual(error.date.hget(), '2012-09-29')
        self.assertEqual(error.time.hget(), '22:58:56')
        self.assertEqual(error.datetime, when)
        self.assertEqual(list(Error.collection(date='2012-09-29')), [error.pk.get()])

    def test_add_error_should_store_the_traceback(self):
        job = Job.add_job(identifier='job:1', queue_name='test')

        try:
            foo = bar
        except Exception, e:
            trace = traceback.format_exc()

        error = Error.add_error(queue_name='test', job=job, error=e, trace=trace)
        self.assertIn("NameError: global name 'bar' is not defined", error.traceback.hget())

    def test_extended_error_can_accept_other_fields(self):
        class ExtendedError(Error):
            namespace = 'test-errortests'
            foo = fields.StringField()
            bar = fields.StringField()

        e = ErrorTests.ExceptionWithCode('the answer', 42)
        job = Job.add_job(identifier='job:1', queue_name='test')

        # create a new error
        error = ExtendedError.add_error(queue_name='test', job=job,
                                        error=e, foo='FOO', bar='BAR')
        self.assertEqual(error.foo.get(), 'FOO')
        self.assertEqual(error.bar.get(), 'BAR')
