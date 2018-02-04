# Changelog

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
