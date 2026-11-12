"""Error contract of the messenger API (issue #45)

Whatever goes wrong underneath, the messenger API reports it with a
`ServerishError`: exceptions of the NATS client library never leave
serverish raw. The tests inject client-library failures at the seam
between serverish and nats-py and check what comes out of the public API.
"""
from __future__ import annotations

import contextlib

import nats.errors
import nats.js.errors
import pytest

from serverish.base import (ServerishError, MessengerNotConnected, MessengerTransportError,
                            MessengerConnectionClosed, MessengerOutboundBufferFull, MessengerJetStreamError,
                            MessengerTransportTimeout, MessengerPublishAckTimeout, MessengerRequestTimeout,
                            MessengerReaderConfigError)
from serverish.connection.nats_errors import (translate_nats_error, nats_errors_translated,
                                              jetstream_error_code)
from serverish.messenger import (Messenger, get_publisher, get_reader, get_corepublisher, get_rpcrequester,
                                 get_kvstore, single_read)

# client-library failure -> what serverish reports
TRANSLATIONS = [
    (nats.errors.ConnectionClosedError, MessengerConnectionClosed),
    (nats.errors.ConnectionDrainingError, MessengerConnectionClosed),
    (nats.errors.ConnectionReconnectingError, MessengerConnectionClosed),
    (nats.errors.StaleConnectionError, MessengerConnectionClosed),
    (nats.errors.UnexpectedEOF, MessengerConnectionClosed),
    (nats.errors.NoServersError, MessengerConnectionClosed),
    (nats.errors.OutboundBufferLimitError, MessengerOutboundBufferFull),
    (nats.js.errors.ServiceUnavailableError, MessengerJetStreamError),
    (nats.js.errors.ServerError, MessengerJetStreamError),
    (nats.js.errors.NotFoundError, MessengerJetStreamError),
    (nats.errors.TimeoutError, MessengerTransportTimeout),
    (nats.js.errors.FetchTimeoutError, MessengerTransportTimeout),
    (nats.errors.MaxPayloadError, MessengerTransportError),
    (nats.errors.BadSubjectError, MessengerTransportError),
    (nats.errors.Error, MessengerTransportError),
]


def _raiser(error: Exception):
    async def _raise(*args, **kwargs):
        raise error
    return _raise


@contextlib.contextmanager
def patched(obj, name: str, replacement):
    """Temporarily replaces an attribute of a (shared, session-wide) object"""
    original = getattr(obj, name)
    setattr(obj, name, replacement)
    try:
        yield
    finally:
        setattr(obj, name, original)


def assert_contract(error: BaseException, cause: BaseException | None = None) -> None:
    assert isinstance(error, ServerishError), f"{type(error).__name__} is not a ServerishError"
    assert not isinstance(error, nats.errors.Error), "serverish exceptions do not derive from client-library ones"
    if cause is not None:
        assert error.__cause__ is cause, "the client-library error must be kept as __cause__"


# ---------------------------------------------------------------------------
# Translation unit (no NATS needed)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('nats_cls, expected', TRANSLATIONS, ids=lambda c: c.__name__)
def test_translation_table(nats_cls, expected):
    raw = nats_cls()
    error = translate_nats_error(raw, "Doing something failed")
    assert type(error) is expected
    assert isinstance(error, ServerishError)
    assert str(error).startswith("Doing something failed: "), \
        "the serverish message must not be shadowed by the hardcoded nats-py one"


def test_translation_leaves_foreign_and_serverish_errors_alone():
    for error in (ValueError('x'), KeyError('k'), MessengerNotConnected('nc'), MessengerPublishAckTimeout('t')):
        assert translate_nats_error(error, "ctx") is error


def test_translation_keeps_cause_and_api_details():
    raw = nats.js.errors.ServiceUnavailableError(code=503, err_code=10008, description='jetstream down')
    with pytest.raises(MessengerJetStreamError) as ei:
        with nats_errors_translated("Asking JetStream failed"):
            raise raw
    assert ei.value.__cause__ is raw
    assert (ei.value.code, ei.value.err_code, ei.value.description) == (503, 10008, 'jetstream down')


def test_api_error_code_derived_from_class_when_missing():
    # the client library raises some API errors without filling `code` in
    assert jetstream_error_code(nats.js.errors.NotFoundError()) == 404
    assert jetstream_error_code(nats.js.errors.BadRequestError()) == 400
    assert jetstream_error_code(translate_nats_error(nats.js.errors.NotFoundError(), "ctx")) == 404
    assert jetstream_error_code(ValueError()) is None


def test_translated_serverish_error_passes_through_context_manager():
    original = MessengerRequestTimeout("already serverish")
    with pytest.raises(MessengerRequestTimeout) as ei:
        with nats_errors_translated("ctx"):
            raise original
    assert ei.value is original


def test_connection_closed_is_not_connected():
    """`except MessengerNotConnected` handles a connection lost on the way, too"""
    assert issubclass(MessengerConnectionClosed, MessengerNotConnected)


def test_serverish_exceptions_are_independent_of_the_client_library():
    """No serverish exception derives from a nats-py one: `except nats.errors.Error`
    does not (and must not be relied on to) catch serverish failures"""
    import inspect
    import serverish.base.exceptions as exc_mod
    for _name, cls in inspect.getmembers(exc_mod, inspect.isclass):
        assert not issubclass(cls, nats.errors.Error), f"{cls.__name__} derives from nats-py"
    for cls in (MessengerTransportTimeout, MessengerPublishAckTimeout, MessengerRequestTimeout):
        assert issubclass(cls, TimeoutError)
        assert str(cls("own message")) == "own message"


def test_messenger_not_opened_is_serverish_error():
    m = Messenger()
    with patched(m, 'conn', None):
        with pytest.raises(MessengerNotConnected):
            _ = m.connection


# ---------------------------------------------------------------------------
# JetStream publisher
# ---------------------------------------------------------------------------

@pytest.mark.nats
@pytest.mark.parametrize('nats_cls, expected', TRANSLATIONS, ids=lambda c: c.__name__)
async def test_publish_never_leaks_client_errors(messenger, unique_subject, nats_cls, expected):
    if issubclass(nats_cls, nats.errors.TimeoutError):
        expected = MessengerPublishAckTimeout  # a publish timeout is a missing ack
    raw = nats_cls()
    pub = get_publisher(unique_subject)
    pub.ack_timeout_retries = 0
    async with pub:
        with patched(pub.connection.js, 'publish', _raiser(raw)):
            with pytest.raises(Exception) as ei:
                await pub.publish(data={'v': 1})
    assert type(ei.value) is expected
    assert_contract(ei.value, cause=raw)
    assert pub.health_status['error_count'] == 1
    assert isinstance(pub._last_error, ServerishError)


@pytest.mark.nats
async def test_publish_error_reported_in_message_when_not_raising(messenger, unique_subject):
    pub = get_publisher(unique_subject)
    pub.raise_on_publish_error = False
    async with pub:
        with patched(pub.connection.js, 'publish', _raiser(nats.errors.ConnectionClosedError())):
            msg = await pub.publish(data={'v': 1})
    assert 'error' in msg['meta']['tags']
    assert 'connection closed' in msg['meta']['status']


@pytest.mark.nats
async def test_publish_not_connected(messenger, unique_subject):
    """No JetStream: MessengerNotConnected, subject to raise_on_publish_error like any other failure"""
    pub = get_publisher(unique_subject)
    async with pub:
        with patched(pub.connection, 'js', None):
            with pytest.raises(MessengerNotConnected):
                await pub.publish(data={'v': 1})

            pub.raise_on_publish_error = False
            msg = await pub.publish(data={'v': 1})
    assert 'error' in msg['meta']['tags']
    assert 'not connected' in msg['meta']['status']
    assert pub.health_status['error_count'] == 2


@pytest.mark.nats
async def test_publish_programming_error_is_not_masked(messenger, unique_subject):
    """Only client-library errors are translated; a bug stays a bug (and visible)"""
    pub = get_publisher(unique_subject)
    async with pub:
        with patched(pub.connection.js, 'publish', _raiser(AttributeError("a bug"))):
            with pytest.raises(AttributeError):
                await pub.publish(data={'v': 1})


# ---------------------------------------------------------------------------
# Core NATS publisher
# ---------------------------------------------------------------------------

@pytest.mark.nats
async def test_core_publish_never_leaks_client_errors(messenger):
    raw = nats.errors.ConnectionClosedError()
    pub = get_corepublisher('test_no_js.error_contract.core')
    async with pub:
        with patched(pub.connection.nc, 'publish', _raiser(raw)):
            with pytest.raises(MessengerConnectionClosed) as ei:
                await pub.publish(data={'v': 1})
            assert_contract(ei.value, cause=raw)

            pub.raise_on_publish_error = False
            msg = await pub.publish(data={'v': 1})
            assert 'error' in msg['meta']['tags']

        with patched(pub.connection, 'nc', None):
            msg = await pub.publish(data={'v': 1})
            assert 'error' in msg['meta']['tags'], "not connected is subject to raise_on_publish_error"
            pub.raise_on_publish_error = True
            with pytest.raises(MessengerNotConnected):
                await pub.publish(data={'v': 1})


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

@pytest.mark.nats
@pytest.mark.parametrize('nats_cls, expected', [
    (nats.errors.TimeoutError, MessengerRequestTimeout),
    (nats.errors.ConnectionClosedError, MessengerConnectionClosed),
    (nats.js.errors.ServiceUnavailableError, MessengerJetStreamError),
], ids=lambda c: c.__name__)
async def test_reader_open_never_leaks_client_errors(messenger, unique_subject, nats_cls, expected):
    raw = nats_cls()
    reader = get_reader(unique_subject, deliver_policy='all')
    with patched(reader.connection.js, 'pull_subscribe', _raiser(raw)):
        with pytest.raises(Exception) as ei:
            await reader.open()
    assert type(ei.value) is expected
    assert_contract(ei.value, cause=raw)
    assert not reader.is_open


@pytest.mark.nats
async def test_single_read_open_timeout_is_serverish_error(messenger, unique_subject):
    """Issue #18: a timeout creating the consumer leaked as nats.errors.TimeoutError"""
    with patched(Messenger().connection.js, 'pull_subscribe', _raiser(nats.errors.TimeoutError())):
        with pytest.raises(MessengerRequestTimeout) as ei:
            await single_read(unique_subject)
    assert_contract(ei.value)
    assert isinstance(ei.value, TimeoutError)


@pytest.mark.nats
async def test_reader_raise_mode_never_leaks_client_errors(messenger, unique_subject):
    raw = nats.errors.ConnectionClosedError()
    reader = get_reader(unique_subject, deliver_policy='all', error_behavior='RAISE')
    async with reader:
        with patched(reader, 'fetch_available', _raiser(raw)):
            with pytest.raises(MessengerConnectionClosed) as ei:
                await reader.read_next()
    assert_contract(ei.value, cause=raw)


async def test_reader_fatal_classification_sees_translated_errors():
    """open() reports a rejected consumer as MessengerJetStreamError(400/404) -
    the read loop must still recognize it as fatal instead of retrying forever."""
    reader = get_reader('test.unit.error_contract', deliver_policy='all', error_behavior='WAIT')

    async def _rejected_open():
        with nats_errors_translated("Creating consumer failed"):
            raise nats.js.errors.NotFoundError()

    reader.open = _rejected_open
    with pytest.raises(MessengerReaderConfigError):
        await reader.read_next()


# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------

@pytest.mark.nats
@pytest.mark.parametrize('nats_cls, expected', [
    (nats.errors.TimeoutError, MessengerRequestTimeout),
    (nats.errors.ConnectionClosedError, MessengerConnectionClosed),
], ids=lambda c: c.__name__)
async def test_rpc_request_never_leaks_client_errors(messenger, nats_cls, expected):
    raw = nats_cls()
    requester = get_rpcrequester('test_no_js.error_contract.rpc')
    async with requester:
        with patched(requester.connection.nc, 'request', _raiser(raw)):
            with pytest.raises(Exception) as ei:
                await requester.request(data={'a': 1}, timeout=1)
    assert type(ei.value) is expected
    assert_contract(ei.value, cause=raw)


# ---------------------------------------------------------------------------
# KV
# ---------------------------------------------------------------------------

@pytest.mark.nats_js
async def test_kv_never_leaks_client_errors(messenger):
    import uuid
    bucket = f"test-kv-{uuid.uuid4().hex[:8]}"
    raw = nats.js.errors.ServiceUnavailableError()
    store = get_kvstore(bucket, create_bucket=True)
    try:
        async with store:
            for operation, args in (('put', ('k', {'v': 1})), ('get', ('k',)), ('delete', ('k',)),
                                    ('purge', ('k',)), ('keys', ()), ('history', ('k',)), ('status', ())):
                with patched(store.kv, operation, _raiser(raw)):
                    with pytest.raises(MessengerJetStreamError) as ei:
                        await getattr(store, operation)(*args)
                assert_contract(ei.value, cause=raw)
                assert ei.value.code == 503
    finally:
        await Messenger().connection.js.delete_key_value(bucket)
