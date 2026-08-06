class ServerishError(Exception):
    """Common base for all serverish exceptions

    Catch this to handle any error originating from the serverish library,
    regardless of the subsystem (messenger, connection, KV). Concrete
    exceptions additionally inherit fitting standard bases (ValueError,
    TimeoutError, KeyError...), so idiomatic generic handlers keep working.
    """
    pass


class MessengerCannotConnect(ServerishError):
    pass

class MessengerNotConnected(ServerishError):
    pass

class MessengerReaderStopped(ServerishError):
    pass

class MessengerReaderConfigError(ServerishError, ValueError):
    """Raised when a MsgReader is misconfigured (e.g. missing required start marker).

    This is a fatal error — the same configuration will never succeed against
    the NATS server.  The reader stops and this exception propagates out of
    ``async for`` loops so callers fail loudly instead of retrying forever.
    """
    pass

class MessengerReaderAlreadyOpen(ServerishError, RuntimeError):
    pass

class MessengerRequestNoResponse(ServerishError):
    pass

class MessengerRequestNoResponders(MessengerRequestNoResponse):
    pass

class MessengerRequestJetStreamSubject(MessengerRequestNoResponse):
    def __init__(self, subject:str) -> None:
        super().__init__(f'Subject {subject} probably declared in JetStream stream. '
                         f'Use pure NATS core subjects for RPC')


class MessengerRequestCanceled(MessengerRequestNoResponse):
    pass

class MessengerRequestTimeout(MessengerRequestNoResponse, TimeoutError):
    pass

class MessengerRequestNoResultYet(MessengerRequestNoResponse):
    pass


class MessengerPublishAckTimeout(ServerishError, TimeoutError):
    """Raised when JetStream did not confirm a publish within the ack timeout.

    The message MAY have been delivered and stored — only the acknowledgement
    did not arrive (or was not processed) in time. A starved client event loop
    is the most common cause, not an actual delivery failure. Publishes carry
    a ``Nats-Msg-Id`` header, so JetStream deduplicates retries of the same
    message within the stream's duplicate window.
    """
    pass


class MessengerKvError(ServerishError):
    pass


class MessengerKvBucketNotFound(MessengerKvError):
    """Raised when a KV bucket does not exist and the driver was not allowed to create it."""
    pass


class MessengerKvKeyNotFound(MessengerKvError, KeyError):
    pass


class MessengerKvMalformed(MessengerKvError):
    """Raised when a KV entry does not carry a serverish envelope.

    Serverish stores full ``{"data": ..., "meta": ...}`` envelopes in KV buckets.
    An entry written by a non-serverish client fails loudly instead of being
    silently returned as empty.
    """
    pass
