Changelog
=========

Release *v2* - ``2019-10-13``
-----------------------------
* Support for limpyd >= 2 only (redis-py >= 3, redis-server >= 3)

Release *v1.1* - ``2019-10-12``
-------------------------------
* Official support for Python 3.7 and 3.8
* Remove support for Python 3.4
* Support redis-limpyd versions >= 1.3.1, < 2
* Support redis-limpyd-extensions >= 1.1.1, < 2

Release *v1.0*  ``2018-01-31``
------------------------------
* First stable version
* Support python versions 2.7 and 3.4 to 3.6
* Support redis-py versions >= 2.10
* Support redis-limpyd versions >= 1.2
* Support redis-limpyd-extensions >= 1.0
* Add indexed date_time field on Error model

Release *v0.1.5*  ``2016-12-25``
--------------------------------

* Correct bug avoiding using a custom WorkerConfig class

Release *v0.1.4*  ``2016-02-05``
--------------------------------

* Correct a bug in `_get_iterable_for_names`

Release *v0.1.3*  ``2015-12-16``
--------------------------------

* Compatibility with Redis-py 2.10
* Accept queues from command line as unicode in python 2

Release *v0.1.2*  ``2015-06-12``
--------------------------------

* Compatibility with pip 6+

Release *v0.1.1*  ``2015-01-12``
--------------------------------

* Stop requiring pip to process dependency links

Release *v0.1.0*  ``2014-09-07``
--------------------------------

* Adding support for python 3.3 and 3.4

Release *v0.0.18*  ``2014-02-21``
---------------------------------

* Log traceback in case of major failures

Release *v0.0.17*  ``2014-02-20``
---------------------------------

* Ability to delay a job before its execution
* Ability to cancel or delay of job during its execution
* Ability to cancel the requeuing of jobs in error

Release *v0.0.16*  ``2014-02-17``
---------------------------------

* `delayed_for` can be a long too (not only int, float or timedelta)

Release *v0.0.15*  ``2014-02-07``
---------------------------------

* Ability to cancel a delayed job
* Helper to retrieve errors assiociated to a job

Release *v0.0.14*  ``2014-01-02``
---------------------------------

* Manage consequences of race condition in get_queue

Release *v0.0.13*  ``2013-12-30``
---------------------------------

* Correct a bug when add_job try to manage duplicate jobs

Release *v0.0.12*  ``2013-11-22``
---------------------------------

* Manage failures when a worker is requeuing delayed jobs
* Add job's PK in worker logs
* Delete the "queued" flag of a job if failure during add_job

Release *v0.0.11*  ``2013-11-21``
---------------------------------

* Add a `queued` field to the `Job` model to enhance avoiding multiple same jobs to be added

Release *v0.0.10*  ``2013-11-18``
---------------------------------

* Add missing scripts module in package

Release *v0.0.9*  ``2013-10-02``
--------------------------------

* Correct bug passing queue_model/error_model/callback to worker script
* Tweak logging

Release *v0.0.8*  ``2013-09-30``
--------------------------------

* A worker can now manage many queues with jobs from many models
* No need to pass `job_model` as it's saved in queues with the jobs pks
* More methods to enhance jobs (`on_*`)
* Add a `max_duration` argument to the worker
* Correct dry-run mode

Release *v0.0.7*  ``2013-09-16``
--------------------------------

* Config for nosetests
* Full test coverage
* Correct a bug with delayed jobs if many workers

Release *v0.0.6*  ``2013-09-15``
--------------------------------

* Jobs can be delayed (when created and/or requeued in case or error)

Release *v0.0.5*  ``2013-09-08``
--------------------------------

* Jobs can be requeued in case of error

Release *v0.0.4*  ``2013-09-06``
--------------------------------

* IT's all about documentation

Release *v0.0.3*  ``2013-09-06``
--------------------------------

* Enhance queue fetching and signal handling

Release *v0.0.2*  ``2013-08-27``
--------------------------------

* Enhance logging override possibilities

Release *v0.0.1*  ``2012-10-10``
--------------------------------

* First public version
