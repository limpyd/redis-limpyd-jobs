import unittest
import sys

from limpyd import DEFAULT_CONNECTION_SETTINGS, TEST_CONNECTION_SETTINGS
from limpyd.contrib.database import PipelineDatabase

from limpyd_jobs import STATUSES
from limpyd_jobs.models import BaseJobsModel

test_database = PipelineDatabase(**TEST_CONNECTION_SETTINGS)


class LimpydBaseTest(unittest.TestCase):

    database = test_database

    @property
    def connection(self):
        return self.database.connection

    def setUp(self):
        # Ensure that we are on the right DB before flushing
        current_db_id = self.connection.connection_pool.connection_kwargs['db']
        assert current_db_id != DEFAULT_CONNECTION_SETTINGS['db']
        assert current_db_id == TEST_CONNECTION_SETTINGS['db']
        self.connection.flushdb()

        # make jobs models use the test database
        BaseJobsModel.use_database(self.database)

    def tearDown(self):
        self.connection.flushdb()

    def count_commands(self):
        """
        Helper method to only count redis commands that work on keys (ie ignore
        commands like info...)
        """
        return self.connection.info()['total_commands_processed']

    def assertNumCommands(self, num, func=None, *args, **kwargs):
        """
        A context assert, to use with "with":
            with self.assertNumCommands(2):
                obj.field.set(1)
                obj.field.get()
        """
        context = _AssertNumCommandsContext(self, num)
        if func is None:
            return context

        # Basically emulate the `with` statement here.

        context.__enter__()
        try:
            func(*args, **kwargs)
        except:
            context.__exit__(*sys.exc_info())
            raise
        else:
            context.__exit__(*sys.exc_info())


class _AssertNumCommandsContext(object):
    """
    A context to count commands occured
    """
    def __init__(self, test_case, num):
        self.test_case = test_case
        self.num = num

    def __enter__(self):
        self.starting_commands = self.test_case.count_commands()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            return

        # we remove 1 to ignore the "info" called in __enter__
        final_commands = self.test_case.count_commands() - 1

        executed = final_commands - self.starting_commands

        self.test_case.assertEqual(
            executed, self.num, "%d commands executed, %d expected" % (
                executed, self.num
            )
        )


class LimpydBaseTestTest(LimpydBaseTest):
    """
    Test parts of LimpydBaseTest
    """

    def test_assert_num_commands_is_ok(self):
        with self.assertNumCommands(1):
            # we know that info do only one command
            self.connection.info()

    def test_statuses_dict_is_ok(self):
        self.assertTrue(isinstance(STATUSES, dict))
        for status in ('WAITING', 'RUNNING', 'SUCCESS', 'ERROR', 'CANCELED'):
            value = status[0].lower()
            self.assertEqual(getattr(STATUSES, status), value)
            self.assertEqual(STATUSES.get(status), value)
            self.assertEqual(STATUSES.by_value(value), status)

        self.assertEqual(len(STATUSES.keys()), 5)

        self.assertEqual(STATUSES.by_value('x', 'UNKNOWN'), 'UNKNOWN')

        with self.assertRaises(ValueError):
            STATUSES.by_value('x')

        with self.assertRaises(KeyError):
            STATUSES.UNKNOWN

        with self.assertRaises(KeyError):
            STATUSES['UNKNOWN']
