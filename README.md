# serverish - helpers for server alike projects
[![CI](https://github.com/AkondLab/serverish/actions/workflows/test-ci.yml/badge.svg)](https://github.com/AkondLab/serverish/actions/workflows/test-ci.yml)

## Features
* Component management
* Singletons
* Connections management and diagnostics
* NATS based Messenger — publishers, readers, KV buckets, RPC ([user guide with examples](doc/messenger.md))
* Live documents - auto-updating configuration from NATS

See the [Messenger user guide](doc/messenger.md) and the [`doc` directory](doc/) for more documentation.

## Optional (extras) dependencies
The following extras are available:
* `messenger` - for using NATS based Messenger. Will install `nats-py` package.
* `dns` - for using `aiodns` DNS resolver for connection status diagnostics, it's extracted as an extra dependency because it's not available on all platforms.

In order to use Messenger, you have to add extra 'messenger' to your `project.toml` serveris dependency, e.g.:
```toml
serverish = {git="https://github.com/AkondLab/serverish.git", extras=["messenger"], branch="master"}
```
or specify dpendency with extras in the `pip` convention: `serverish[messenger]`.
this will install `nats-py` package.

## Plans
- Decorators `@messenger.sub(subject)` (Web framework style)
- Progress with automatic increment for known durations e.g. `pro_pub.update(completed=1, when=+20)`
- Custom JetStream API replacing nats-py private internals access

## Changes
* 2.3 Policy objects (`DeliveryPolicy`, `ErrorPolicy`/`RetryPolicy`, `BatchPolicy`) for typed driver
  configuration with backward-compatible string/kwarg forms; dynamic pull batching by default;
  driver lifecycle fixes (closed drivers leave the Messenger tree); deprecates
  `get_singlepublisher`/`get_singlereader` and publishing on never-opened publishers.
  See the [Messenger user guide](doc/messenger.md).
* 2.2 Deterministic end-of-data for `nowait` readers (server-stamped `num_pending`, no fixed waits);
  publish ack-timeout handling with `Nats-Msg-Id` dedup and transparent retries; common
  `ServerishError` exception base. **Use 2.2.1+** — 2.2.0 is yanked (silent message loss, #38).
* 2.1 JetStream KV bucket support: `MsgKvStore`/`MsgKvReader`/`MsgKvSubscriber` with full
  envelope semantics and one-shot `kv_get`/`kv_put`.
* 1.7 Subscription reliability: reader auto-recovers from NATS disconnects and consumer expiry,
  RPC responder auto-resubscribes, `health_status` property on all drivers (reader, publisher,
  RPC responder, connection) with reconnect counts, error tracking, slow consumer detection.
  Comprehensive test suite (194 tests) with testcontainers-based NATS fixtures.
* 1.5 Introduces live documents (`LiveDocument`, `get_documentreader()`, `get_live_document()`) for auto-updating configuration from NATS.
* 1.4 Contains improvements to the network problems recovery and connection tracking.
* 1.1 Switches to `param` 2.* nad `py-nats` 1.7.*. Also, publisher may reraise exceptions or ignore them.
* 1.0  Changes NATS subscriptions to pull only
* 0.12 Extends messges metdata of `nats` section, Handles reconnection of subscriptions, 
* 0.11 Extends, tasks, NATS connection tracking and messenger opening options (`Messenger.open`)
* 0.10 Adds timestamp to journal messages and extablishes schema for journal messages.
* 0.9.1 Makes dependency to aiodns optional.
* 0.9.0 Introduces Logging handler for passing standard logging messages to the Messenger.
* 0.8 Introduces journal publishing/reading API.
* 0.7 Includes progress tracking messages in the Messenger API.
* 0.6 introduces breaking changes in the Messenger API:
  * Convenience functions `get_xxxxx()` returning publishers/readers are synchronous now. Remove `await`-ing for their results.
  * Change of signature of the callback for `MsgRpcResponder.register_function` for `Rpc->None`.  


## Credits
This package uses some code and ideas from the [rich](https://github.com/Textualize/rich) package (Copyright (c) 2020 Will McGugan).

