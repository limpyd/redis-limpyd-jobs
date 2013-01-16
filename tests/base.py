import unittest
import sys
from StringIO import StringIO
from unittest.case import _AssertRaisesContext

from limpyd.database import DEFAULT_CONNECTION_SETTINGS
from limpyd.contrib.database import PipelineDatabase

from limpyd_jobs import STATUSES
from limpyd_jobs.models import BaseJobsModel

TEST_CONNECTION_SETTINGS = DEFAULT_CONNECTION_SETTINGS.copy()
TEST_CONNECTION_SETTINGS['db'] = 15

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

    def assertSystemExit(self, in_stderr=None, in_stdout=None):
        """
        A context to wrap to assertRaises(SystemExit) which can also test (via
        assertIn) the content of stderr and stdout
        """
        return _AssertSystemExit(self, in_stderr, in_stdout)


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


class _AssertSystemExit(_AssertRaisesContext):
    """
    A context to wrap to assertRaises(SystemExit) which can also test (via
    assertIn) the content of stderr and stdout
    """
    def __init__(self, test_case, in_stderr=None, in_stdout=None):
        super(_AssertSystemExit, self).__init__(SystemExit, test_case)
        self.test_case = test_case
        self.in_stderr = in_stderr
        self.in_stdout = in_stdout

    def __enter__(self):
        if self.in_stderr is not None:
            self.old_stderr = sys.stderr
            sys.stderr = self.stderr = StringIO()

        if self.in_stdout is not None:
            self.old_stdout = sys.stdout
            sys.stdout = self.stdout = StringIO()

        super(_AssertSystemExit, self).__enter__()

    def __exit__(self, exc_type, exc_value, tb):
        if self.in_stderr is not None:
            sys.stderr = self.old_stderr

        if self.in_stdout is not None:
            sys.stdout = self.old_stdout

        result = super(_AssertSystemExit, self).__exit__(exc_type, exc_value, tb)
        if not result:
            return result

        if self.in_stderr is not None:
            self.test_case.assertIn(self.in_stderr, self.stderr.getvalue())

        if self.in_stdout is not None:
            self.test_case.assertIn(self.in_stdout, self.stdout.getvalue())

        return True


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
