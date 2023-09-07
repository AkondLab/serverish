# serverish - helpers for server alike projects
[![CI](https://github.com/AkondLab/serverish/actions/workflows/ci.yml/badge.svg)](https://github.com/AkondLab/serverish/actions/workflows/ci.yml)

## Features
* Component management
* Singletons
* Connections management and diagnostics
* NATS
* NATS based Messenger

See [`doc` directory](doc/) for more documentation.

## Optional (extras) dependencies
In order to use Messenger, you have to add extra 'messenger' to your `project.toml` serveris dependency, e.g.:
```toml
serverish = {git="https://github.com/AkondLab/serverish.git", extras=["messenger"], branch="master"}
```
or specify dpendency with extras in the `pip` convention: `serverish[messenger]`.
this will install `nats-py` package.

## Credits
This package uses some code and ideas from the [rich](https://github.com/Textualize/rich) package (Copyright (c) 2020 Will McGugan).

