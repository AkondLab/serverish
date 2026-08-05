class MessengerCannotConnect(Exception):
    pass

class MessengerNotConnected(Exception):
    pass

class MessengerReaderStopped(Exception):
    pass

class MessengerReaderConfigError(ValueError):
    """Raised when a MsgReader is misconfigured (e.g. missing required start marker).

    This is a fatal error — the same configuration will never succeed against
    the NATS server.  The reader stops and this exception propagates out of
    ``async for`` loops so callers fail loudly instead of retrying forever.
    """
    pass

class MessengerReaderAlreadyOpen(RuntimeError):
    pass

class MessengerRequestNoResponse(Exception):
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


class MessengerKvError(Exception):
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