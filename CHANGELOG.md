# Changelog

## Version 0.12.0 (2023-02-25)
* Minimum Python version is now 3.6. Thanks @ronlut!
* Support for RQ 0.13. Thanks @ronlut!
* Added `at_front` parameter to `enqueue_at()`. Thanks @gabriels1234!
* Added support for `on_success` and `on_failure` handler to `enqueue_*` methods. Thanks @andy-takker!
* Replace `croniter` with `crontab` package. Thanks @zentist!

## Version 0.11.0 (2021-03-07)
* `Scheduler` now accepts `queue_class` argument. Thanks @gisce!
* Fixes local timezone handling. Thanks @Eugeny!

## Version 0.10.0 (2020-04-28)
* You can now run multiple schedulers at the same time. Thanks @oxalorg!
* You can now schedule a job with dependency by specifying the `depends_on` kwarg. Thanks @noncomputable!
* Added `use_local_timezone` argument to `scheduler.cron()`. Thanks @Eugeny!

## Version 0.9.1 (2019-10-08)
* `scheduler._create_job()` now respects `queue_name` parameter. Thanks @shubhamdipt!
* Added `queue_name` parameter to `enqueue_at()` and `enqueue_in()`. Thanks @gauravmk!

## Version 0.9 (2018-12-12)
* Compatible with RQ >= 0.13 and redis-py >= 3.0. Thanks @ericatkin and @selwin!
* `scheduler.schedule()` now accepts `meta` argument. Thanks @as3445!
* `scheduler.get_jobs()` now returns an iterator. Thanks @craynic!

## Version 0.8.3 (2018-05-17)
* Fixed lock management error. Thanks @chaffeqa!

## Version 0.8.2 (2018-03-02)
* Fixed conflicting `-q` parameter from rqscheduler script. Thanks @sourcepirate!

## Version 0.8.1 (2018-02-04)
* Fixed packaging error

## Version 0.8.0 (2018-02-04)
* Added support for custom `Job` and `Queue` classes. Thanks @skirsdeda!
* You can now pass in a `Queue` object when instantiating the scheduler as such `Scheduler(queue=queue)`. Thanks @peergradeio!
* Fixes a crash that happens when the scheduler runs for longer than sleep interval. Thanks @chaffeqa!
* Added `job_description`, `job_id`, `job_ttl` and `job_result_ttl ` kwargs to `enqueue_at` and `enqueue_in`. Thanks @bw, @ryanolf and @gusevaleksei!
* You can now run `rqscheduler` with `--quiet` or `-q` flag to silence `INFO` level log messages. Thanks @bw!
* Scheduler will now enqueue jobs at exactly the defined interval. Thanks @hamx0r!
* You can now run multiple schedulers at the same time. Thanks @marcinn!


## Version 0.6.1
* Added `scheduler.count()`. Thanks @smaccona!
* `scheduler.get_jobs()` now supports pagination. Thanks @smaccona!
* Better `ttl` and `result_ttl` defaults for jobs created by `scheduler.cron`. Thanks @csaba-stylight and @lechup!


## Version 0.6.0
* Added `scheduler.cron()` capability. Thanks @petervtzand!
* `scheduler.schedule()` now accepts `id` and `ttl` kwargs. Thanks @mbodock!


## Version 0.5.1
* Travis CI fixes. Thanks Steven Kryskalla!
* Modified default logging configuration. You can pass in the `-v` or `--verbose` argument
  to `rqscheduler` script for more verbose logging.
* RQ Scheduler now registers Queue name when a new job is scheduled. Thanks @alejandrodob !
* You can now schedule jobs with string references like `scheduler.schedule(scheduled_time=now, func='foo.bar')`.
  Thanks @SirScott !
* `rqscheduler` script now accepts floating point intervals. Thanks Alexander Pikovsky!


## Version 0.5.0
* IMPORTANT! Job timestamps are now stored and interpreted in UTC format.
  If you have existing scheduled jobs, you should probably change their timestamp
  to UTC before upgrading to 0.5.0. Thanks @michaelbrooks!
* You can now configure Redis connection via environment variables. Thanks @malthe!
* `rqscheduler` script now accepts `--pid` argument. Thanks @jsoncorwin!


## Version 0.4.0
* Supports Python 3!
* `Scheduler.schedule` now allows job `timeout` to be specified
* `rqscheduler` allows Redis connection to be specified via `--url` argument
* `rqscheduler` now accepts `--path` argument


## Version 0.3.6
* Scheduler key is not set to expire a few seconds after the next scheduling
  operation. This solves the issue of `rqscheduler` refusing to start after
  an unexpected shut down.

## Version 0.3.5
* Support `StrictRedis`


## Version 0.3.4
* Scheduler related job attributes (`interval` and `repeat`) are now stored
  in `job.meta` introduced in RQ 0.3.4


## Version 0.3.3
* You can now check whether a job is scheduled for execution using
  `job in scheduler` syntax
* Added `scheduler.get_jobs` method
* `scheduler.enqueue` and `scheduler.enqueue_periodic` will now raise a
  DeprecationWarning, please use `scheduler.schedule` instead

## Version 0.3.2
* Periodic jobs now require `RQ`_ >= 0.3.1

## Version 0.3
* Added the capability to create periodic (cron) and repeated job using `scheduler.enqueue`
