"""All serverish exceptions share the ServerishError base and keep their
idiomatic standard bases."""
import inspect

import serverish.base.exceptions as exc_mod
from serverish.base.exceptions import ServerishError


def test_all_exceptions_derive_from_serverish_error():
    exceptions = [obj for _name, obj in inspect.getmembers(exc_mod, inspect.isclass)
                  if issubclass(obj, Exception)]
    assert len(exceptions) > 10
    for cls in exceptions:
        assert issubclass(cls, ServerishError), f"{cls.__name__} lacks ServerishError base"


def test_idiomatic_standard_bases_preserved():
    from serverish.base.exceptions import (MessengerKvKeyNotFound, MessengerPublishAckTimeout,
                                           MessengerReaderConfigError, MessengerRequestTimeout)
    assert issubclass(MessengerReaderConfigError, ValueError)
    assert issubclass(MessengerPublishAckTimeout, TimeoutError)
    assert issubclass(MessengerRequestTimeout, TimeoutError)
    assert issubclass(MessengerKvKeyNotFound, KeyError)
