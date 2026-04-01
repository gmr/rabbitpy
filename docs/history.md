# Version History

## 3.0.0 — *2024-05-08*

- Change to use `ssl.SSLContext` since `ssl.wrap_socket` was deprecated in Python 3.7 and removed in 3.12
- Drops support for Python < 3.8
- Adds support for Python 3.10, 3.11, and 3.12
- Change tests over to use `coverage` instead of `nose`

## 2.0.1 — *2019-08-06*

- Fixed an issue with the IO loop poller on MacOS (#111)

## 2.0.0 — *2019-04-19*

- Updated to use pamqp>=2.3,<3:
    - Field table keys are now strings and no longer bytes
    - In Python 2.7 if a short-string has UTF-8 characters, it will be a unicode object
- Field-table integer encoding changes
- Drops support for Python < 3.4
- Adds support for Python 3.6 and 3.7

## 1.0.0 — *2016-10-27*

- Reworked Heartbeat logic to send a heartbeat every `interval / 2` seconds when data has not been written to the socket (#70, #74, #98, #99)
- Improved performance when consuming large messages (#104) — *Jelle Aalbers*
- Allow for username and password to be default again (#96, #97) — *Grzegorz Śliwiński*
- Cleanup of Connection and Channel teardown (#103)

## 0.27.1 — *2016-05-12*

- Fix a bug where the IO write trigger socketpair is not being cleaned up on close

## 0.27.0 — *2016-05-11*

- Added new `SimpleChannel` class
- Exception formatting changes
- Thread locking optimizations
- Connection shutdown cleanup
- `Message` now assigns a UTC timestamp instead of local timestamp if requested (#94)
- Unquote username and password in URI (#93) — *sunbit*
- Connections now allow for a configurable timeout (#84, #85) — *vmarkovtsev*
- Bugfix for `basic_publish` (#92) — *canardleteer*
- Added `args` property to `Connection` (#88) — *vmarkovtsev*
- Fix locale in connection setup from causing hanging (#87) — *vmarkovtsev*
- Fix heartbeat behavior (#69, #70, #74)
- Cancel consuming in case of exceptions (#68) — *kmelnikov*
- Documentation correction (#79) — *jonahbull*

## 0.26.2 — *2015-03-17*

- Fix behavior for Basic.Return frames sent from RabbitMQ
- Pin pamqp 1.6.1 fixing an issue with max-channels

## 0.26.1 — *2015-03-09*

- Add the ability to interrupt rabbitpy when waiting on a frame (#38)
- Use a custom base class for all Exceptions (#57) — *Jeremy Tillman*
- Fix for consumer example in documentation (#60) — *Michael Becker*
- Add `rabbitpy.amqp` module for unopinionated access to AMQP API
- Refactor how client side heartbeat checking is managed
- Address an issue when client side channel max count is not set
- Clean up handling of remote channel and connection closing
- Fix URI query parameter names to match AMQP URI spec
- PYPY behavior fixes related to garbage collection

## 0.25.0 — *2014-12-16*

- Acquire a lock when creating a new channel to fix multi-threaded channel creation (#56)
- Add client side heartbeat checking (#55)
- Fix a bug where Basic.Nack checking was checking for the wrong string
- Add support for Python3 memoryviews for the message body (#50)
- Improve Python3 behavior in `maybe_utf8_encode`

## 0.24.0 — *2014-12-12*

- Update to reflect changes in pamqp 1.6.0

## 0.23.0 — *2014-11-05*

- Fix a bug where message body length was assigned before converting unicode to bytes (#49)
- Fix the automatic coercion of header types to UTF-8 encoded bytes (#49)
- Raise `TypeError` if a timestamp property cannot be converted

## 0.22.0 — *2014-11-04*

- Address an issue when RabbitMQ is configured with a max-frame-size of 0 (#48)
- Do not lose the traceback when exiting a context manager due to an exception (#46)
- Adds server capability checking in `Channel` methods
- Pin pamqp version range to >= 1.4, < 2.0

## 0.21.1 — *2014-10-23*

- Clean up KQueue issues (#44)
- Remove sockets from KQueue when in error state
- Handle socket connect errors more cleanly (#44)

## 0.21.0 — *2014-10-21*

- Address a possible edge case where message frames can be interspersed when publishing in a multi-threaded environment
- Add exception handling around select.error (#43)
- Add a new `opinionated` flag in `Message` construction
- Add wheel distribution

## 0.20.0 — *2014-10-01*

- Added support for KQueue and Poll in IOLoop
- Fixed issues with publishing large messages (#37)
- Add exchange property to `Message` (#40)
- Add out-of-band consumer cancellation with `Queue.stop_consuming()` (#38, #39)
- Significantly increase test coverage

## Earlier Releases

- **0.19.0** — Fix the socket read/write buffer size (#35)
- **0.18.1** — Fix unicode message body encoding in Python 2
- **0.18.0** — Make IO thread daemonic; add `Message.redelivered` property
- **0.17.0** — Refactor cross-thread communication for RabbitMQ invoked RPC methods
- **0.16.0** — Fix an issue with `no_ack=True` consumer cancellation; fix exchange and queue unbinding
- **0.15.0** — Change default durability for Exchange and Queue to False
- **0.14.0** — Add support for `authentication_failure_close`; add consumer priorities
- **0.13.0** — Validate heartbeat is always an integer
- **< 0.5.0** — Previously called *rmqid*
