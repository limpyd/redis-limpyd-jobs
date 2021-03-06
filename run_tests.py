#!/usr/bin/env python

from __future__ import unicode_literals
import sys
import unittest

import argparse

from tests import base, models, workers


if __name__ == "__main__":

    # Define arguments
    parser = argparse.ArgumentParser(description="Run redis-limpyd-jobs tests suite.")
    parser.add_argument(
        "tests",
        nargs="*",
        default=None,
        help="Tests (module, TestCase or TestCaseMethod) to run. "
             "use full path, eg.: `tests.module.Class.test` ; "
             "`tests.module.Class` works also ; `tests.module` too!"
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        type=int,
        action="store",
        dest="verbosity",
        default=2,
        help="Verbosity of the runner."
    )
    args = parser.parse_args()

    if args.tests:
        # we have names
        suite = unittest.TestLoader().loadTestsFromNames(args.tests)
    else:
        # Run all the tests
        suites = []
        for mod in [base, models, workers]:
            suite = unittest.TestLoader().loadTestsFromModule(mod)
            suites.append(suite)
        suite = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=args.verbosity).run(suite)
