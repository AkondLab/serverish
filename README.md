# serverish - helpers for server alike projects
[![CI](https://github.com/AkondLab/serverish/actions/workflows/ci.yml/badge.svg)](https://github.com/AkondLab/serverish/actions/workflows/ci.yml)

## Features
* Component management
* Singletons
* Connections management and diagnostics
* NATS based Messenger

See [`doc` directory](doc/) for more documentation.

## Optional (extras) dependencies
In order to use Messenger, you have to add extra 'messenger' to your `project.toml` serveris dependency, e.g.:
```toml
serverish = {git="https://github.com/AkondLab/serverish.git", extras=["messenger"], branch="master"}
```
or specify dpendency with extras in the `pip` convention: `serverish[messenger]`.
this will install `nats-py` package.

## Changes
* 0.8 Introduces journal publishing/reading API.
* 0.7 Includes progress tracking messages in the Messenger API.
* 0.6 introduces breaking changes in the Messenger API:
  * Convenience functions `get_xxxxx()` returning publishers/readers are synchronous now. Remove `await`-ing for their results.
  * Change of signature of the callback for `MsgRpcResponder.register_function` for `Rpc->None`.  


## Credits
This package uses some code and ideas from the [rich](https://github.com/Textualize/rich) package (Copyright (c) 2020 Will McGugan).

