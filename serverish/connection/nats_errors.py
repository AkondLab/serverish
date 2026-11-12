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
