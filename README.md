# serverish - helpers for server alike projects
[![CI](https://github.com/AkondLab/serverish/actions/workflows/test-ci.yml/badge.svg)](https://github.com/AkondLab/serverish/actions/workflows/test-ci.yml)

## Features
* Component management
* Singletons
* Connections management and diagnostics
* NATS based Messenger

See [`doc` directory](doc/) for more documentation.

## Optional (extras) dependencies
Following extras are available:
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
- Pytest fixtures for NATS on CI

## Changes
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

