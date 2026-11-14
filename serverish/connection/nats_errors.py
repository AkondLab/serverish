"""Translation of NATS client library errors into serverish exceptions

The single place where ``nats.errors`` / ``nats.js.errors`` exceptions are
mapped onto the `ServerishError` hierarchy. The messenger API never lets a
raw client-library exception out: whoever catches `ServerishError` handles
every failure serverish can report, without knowing which client library
(or which version of it) works underneath.

Use `nats_errors_translated` around calls into the client library at the
public boundary of a driver. Code which reacts to specific client errors
internally keeps catching the raw exceptions inside that boundary.
"""
from __future__ import annotations

import contextlib
from typing import Iterator

import nats.errors
import nats.js.errors

from serverish.base.exceptions import (ServerishError, MessengerTransportError, MessengerConnectionClosed,
                                       MessengerOutboundBufferFull, MessengerJetStreamError,
                                       MessengerTransportTimeout)

_CONNECTION_GONE = (
    nats.errors.ConnectionClosedError,
    nats.errors.ConnectionDrainingError,
    nats.errors.ConnectionReconnectingError,
    nats.errors.StaleConnectionError,
    nats.errors.UnexpectedEOF,  # a StaleConnectionError subclass today - listed so the mapping does not depend on it
    nats.errors.NoServersError,
)

# Status implied by the class of a JetStream API error; the client library
# does not always fill `code` in (e.g. errors it raises on its own)
_API_ERROR_CODES = (
    (nats.js.errors.NotFoundError, 404),
    (nats.js.errors.BadRequestError, 400),
    (nats.js.errors.ServiceUnavailableError, 503),
    (nats.js.errors.ServerError, 500),
)


def translate_nats_error(error: Exception, context: str) -> Exception:
    """Returns the serverish counterpart of a NATS client library exception

    Args:
        error: exception to translate
        context: what serverish was doing, the lead of the resulting message

    Returns:
        A `MessengerTransportError` (subclass) for a client library error -
        raise it ``from error`` to keep the original as ``__cause__``.
        Any other exception (serverish ones included) is returned unchanged.
    """
    if isinstance(error, ServerishError) or not isinstance(error, nats.errors.Error):
        return error
    message = f"{context}: {error}"
    if isinstance(error, _CONNECTION_GONE):
        return MessengerConnectionClosed(message)
    if isinstance(error, nats.errors.OutboundBufferLimitError):
        return MessengerOutboundBufferFull(message)
    if isinstance(error, nats.js.errors.APIError):
        return MessengerJetStreamError(message, code=jetstream_error_code(error), err_code=error.err_code,
                                       description=error.description)
    if isinstance(error, nats.errors.TimeoutError):
        return MessengerTransportTimeout(message)
    return MessengerTransportError(message)


@contextlib.contextmanager
def nats_errors_translated(context: str) -> Iterator[None]:
    """Context manager translating NATS client library errors raised in its body

    Args:
        context: what serverish is doing, the lead of the resulting message

    Raises:
        MessengerTransportError: (subclass) instead of any ``nats.errors.Error``
    """
    try:
        yield
    except nats.errors.Error as e:
        raise translate_nats_error(e, context) from e


#: JetStream API `err_code` of "stream not found"
_ERR_STREAM_NOT_FOUND = 10059


def is_missing_stream(error: BaseException | None) -> bool:
    """Tells whether a raw client library error says that the stream is not there

    A 404 from JetStream names what is missing by its `err_code`. The stream missing is
    a deployment error; a consumer missing (10014) is ordinary life of ephemeral consumers.
    The client library raises the 404 on its own, with no `err_code`, in one place a reader
    can meet: when it finds no stream storing the subject.
    """
    return (isinstance(error, nats.js.errors.NotFoundError)
            and error.err_code in (None, _ERR_STREAM_NOT_FOUND))


#: JetStream API `err_code` of "maximum consumers limit reached"
_ERR_CONSUMER_LIMIT = 10026


def is_consumer_limit(error: BaseException | None) -> bool:
    """Tells whether a raw client library error says that the stream takes no more consumers

    The server reports it as a 400, like a wrong configuration, but it is not one: a slot
    is freed when any consumer of the stream goes away.
    """
    return isinstance(error, nats.js.errors.BadRequestError) and error.err_code == _ERR_CONSUMER_LIMIT


def jetstream_error_code(error: Exception) -> int | None:
    """Returns the HTTP-like status code of a JetStream API error, raw or translated"""
    if isinstance(error, MessengerJetStreamError):
        return error.code
    if isinstance(error, nats.js.errors.APIError):
        if error.code is not None:
            return int(error.code)
        for cls, code in _API_ERROR_CODES:
            if isinstance(error, cls):
                return code
    return None
