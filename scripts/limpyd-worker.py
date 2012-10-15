#!/usr/bin/env python

import sys
from optparse import OptionParser, make_option


class LaxOptionParser(OptionParser):
    """
    An option parser that doesn't raise any errors on unknown options.

    This is needed because the --pythonpath is needed before looking for
    wanted config class to use.
    """
    def error(self, msg):
        pass

    def print_help(self):
        """Output nothing.

        The lax options are included in the normal option parser, so under
        normal usage, we don't need to print the lax options.
        """
        pass

    def print_lax_help(self):
        """Output the basic options available to every command.

        This just redirects to the default print_help() behaviour.
        """
        OptionParser.print_help(self)

    def _process_args(self, largs, rargs, values):
        """
        Overrides OptionParser._process_args to exclusively handle default
        options and ignore args and other options.

        This overrides the behavior of the super class, which stop parsing
        at the first unrecognized option.
        """
        while rargs:
            arg = rargs[0]
            try:
                if arg[0:2] == "--" and len(arg) > 2:
                    # process a single long option (possibly with value(s))
                    # the superclass code pops the arg off rargs
                    self._process_long_opt(rargs, values)
                elif arg[:1] == "-" and len(arg) > 1:
                    # process a cluster of short options (possibly with
                    # value(s) for the last one only)
                    # the superclass code pops the arg off rargs
                    self._process_short_opts(rargs, values)
                else:
                    # it's either a non-default option or an arg
                    # either way, add it to the args list so we can keep
                    # dealing with options
                    del rargs[0]
                    raise Exception
            except:
                largs.append(arg)


if __name__ == '__main__':
    # first options needed for this script itself (the rest will be ignored for now)
    option_list = (
        make_option('--pythonpath', action='append',
            help='A directory to add to the Python path, e.g. --pythonpath=/my/module'),
        make_option('--worker-config', dest='worker_config',
            help='The worker config class to use, e.g. --worker-config=my.module.MyWorkerConfig, '
                  'default to limpyd_jobs.workers.WorkerConfig')
    )

    # create a light option parser that ignore everything but basic options
    # defined below
    parser = LaxOptionParser(usage="%prog [options]", option_list=option_list)
    options, args = parser.parse_args(sys.argv[:])

    # if you have some pythonpaths, add them
    if options.pythonpath:
        for path in options.pythonpath:
            sys.path.insert(0, path)

    # still load the defaut config, needed to parse the worker_config option
    from limpyd_jobs.workers import WorkerConfig

    # by default use the default worker config
    worker_config_class = WorkerConfig

    # and try to load the one passed as argument if any
    if options.worker_config:
        worker_config_class = WorkerConfig.import_class(options.worker_config)

    # finally instantiate and run the worker
    worker_config = worker_config_class()
    worker_config.execute()
