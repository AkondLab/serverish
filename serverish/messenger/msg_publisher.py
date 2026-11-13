from __future__ import annotations

import asyncio
import time
import warnings

import jsonschema
import nats.errors
import nats.js
import param

from serverish.base import MessengerNotConnected, MessengerNotPersisted, MessengerPublishAckTimeout
from serverish.connection.nats_errors import translate_nats_error
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, log
from serverish.messenger.policies import UNLIMITED, ErrorPolicy, OnError, RetryPolicy


class MsgPublisher(MsgDriver):
    """A class for publishing data to a Messenger subject

    Use this class if you want to publish data to a messenger subject.
    Check for specialist publishers for common use cases.

    Every publish carries a ``Nats-Msg-Id`` header equal to the message
    ``meta.id``, so JetStream deduplicates repeated deliveries of the same
    message within the stream's duplicate window. This makes retrying after
    an ack timeout safe: if the original publish did reach the stream (a
    common case when the client event loop is starved and the ack is simply
    processed too late), the retry is discarded by the server.

    The publisher is meant for *persistent* subjects: messages published are
    stored by the server and can be read later (for fire-and-forget messages
    on volatile subjects use `MsgCorePub`). With ``persistent=True`` this is a
    guarantee: `open()` verifies that the subject is persistent and `publish()`
    fails with `MessengerNotPersisted` if the message was not stored. Without
    it (the default until serverish 3.0) a message nobody stored is reported
    with a warning, once per publisher, and the publish counts as done.

    Parameters:
        persistent (bool): Require the subject to be persistent (default `False`, `True` in serverish 3.0)
        raise_on_publish_error (bool): Raise on publish error (default `True`). When `False`, errors
            (a missing connection included) are logged and reported in the returned message instead
        ack_timeout_retries (int): How many times to retry a publish for which no JetStream
            ack arrived in time (default 2). Retries are deduplicated server-side via
            ``Nats-Msg-Id``. When retries are exhausted, `MessengerPublishAckTimeout` is
            raised (subject to `raise_on_publish_error`).

    Health monitoring:
        The publisher tracks publish statistics accessible via `health_status` property:
        - publish_count: Total successful publishes
        - error_count: Total publish errors
        - last_publish_time: Timestamp of last successful publish
        - last_error: Last error encountered
    """

    persistent = param.Boolean(default=False, doc="Require the subject to be persistent: fail on open() and "
                                                  "publish() if published messages are not stored")
    raise_on_publish_error = param.Boolean(default=True, doc="Raise on publish error")
    ack_timeout_retries = param.Integer(default=2, bounds=(0, None),
                                        doc="Retries when JetStream ack does not arrive in time; "
                                            "safe thanks to Nats-Msg-Id deduplication")

    #: Effective ack-retry pacing (docs/design/POLICIES.md §1.4: driver defaults
    #: live at the driver). Overridable via error_policy=ErrorPolicy(retry=...).
    retry_defaults = RetryPolicy(attempts=2, delay=0.0, backoff=1.0,
                                 max_delay=UNLIMITED, total_timeout=UNLIMITED)

    def __init__(self, error_policy: ErrorPolicy | None = None, **kwargs) -> None:
        # --- policy normalization (docs/design/POLICIES.md) ---
        self.errors: ErrorPolicy | None = ErrorPolicy.from_kwargs(
            error_policy,
            raise_on_publish_error=kwargs.get('raise_on_publish_error'),
            ack_timeout_retries=kwargs.get('ack_timeout_retries'))
        if self.errors is not None and self.errors.on_error is not None:
            if self.errors.on_error not in (OnError.RAISE, OnError.LOG):
                raise ValueError("Publishers support OnError.RAISE or OnError.LOG only")
            kwargs['raise_on_publish_error'] = self.errors.on_error is OnError.RAISE
        if (self.errors is not None and self.errors.retry is not None
                and self.errors.retry.attempts is not None
                and self.errors.retry.attempts != UNLIMITED):  # equality, not identity: float('inf') is a distinct object
            kwargs['ack_timeout_retries'] = int(self.errors.retry.attempts)
        # --- end policy normalization ---
        # Health monitoring fields
        self._publish_count: int = 0
        self._error_count: int = 0
        self._last_publish_time: float | None = None
        self._last_error: Exception | None = None
        self._warned_unopened: bool = False
        self._was_opened: bool = False
        self._warned_not_persistent: bool = False
        super().__init__(**kwargs)

    async def open(self) -> None:
        """Opens the publisher; with ``persistent=True`` verifies that the subject is persistent

        Raises:
            MessengerNotPersisted: (``persistent=True`` only) messages published to the subject would not be stored
            MessengerNotConnected: (``persistent=True`` only) no connection to verify through
            MessengerTransportError: (``persistent=True`` only) the server could not be asked
        """
        if self.persistent and not await self.messenger.is_persistent(self.subject):
            raise MessengerNotPersisted(self.subject)
        self._was_opened = True
        await super().open()

    @property
    def _ack_retry(self) -> RetryPolicy:
        """Resolved ack-retry pacing: error_policy.retry wins; otherwise the
        legacy `ack_timeout_retries` param (kept live - it may be set after
        construction) fills the attempts slot of the driver defaults."""
        if self.errors is not None and self.errors.retry is not None:
            return self.errors.retry.resolved(self.retry_defaults)
        return RetryPolicy(attempts=self.ack_timeout_retries).resolved(self.retry_defaults)

    async def publish(self, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
        """Publishes a messages to publisher subject

        Args:
            data (dict): message data
            meta (dict): message metadata
            kwargs: additional arguments to pass to the connection
        Returns:
            dict: published message, or message to be published with the `error` tag in meta if failed
        Raises:
            MessengerNotConnected: no connection to publish through
            MessengerNotPersisted: (``persistent=True`` only) the message was not stored
            MessengerPublishAckTimeout: JetStream did not confirm the publish (after retries)
            MessengerTransportError: any other failure of the underlying NATS client
            jsonschema.ValidationError: the message does not match its schema (always raised)

            With `raise_on_publish_error` set to False the serverish errors above are not raised:
            the error is logged and the message is returned with the `error` tag and `status`
            set to the error message.
        """
        if not self.is_open and not self._warned_unopened:
            self._warned_unopened = True
            state = "closed" if self._was_opened else "never-opened"
            warnings.warn(
                f"Publishing on a {state} publisher ({self}). Open it explicitly "
                f"(await pub.open() or 'async with pub:') - publishing on a publisher "
                f"that is not open will become an error in serverish 3.0; "
                f"for one-shot use single_publish()",
                DeprecationWarning, stacklevel=2)
        msg = self.messenger.create_msg(data, meta)
        bdata = self.messenger.encode(msg)
        try:
            self.messenger.msg_validate(msg)
        except jsonschema.ValidationError as e:
            log.error(f"Message {msg['meta']['id']} validation error: {e}")
            raise e
        self.messenger.log_msg_trace(msg['data'], msg['meta'], f"PUB to {self.subject}")
        headers = dict(kwargs.pop('headers', None) or {})
        # Server-side dedup id: makes ack-timeout retries safe. Must be globally
        # unique, hence the per-process instance prefix - meta.id alone is only
        # process-locally unique and would collide across processes/restarts,
        # silently dropping messages within the stream's duplicate window.
        headers.setdefault('Nats-Msg-Id', f"{self.messenger.instance_id}:{msg['meta']['id']}")
        try:
            await self._publish_with_ack_retries(bdata, headers, msg['meta']['id'], **kwargs)
            # Track successful publish
            self._publish_count += 1
            self._last_publish_time = time.monotonic()
        except (nats.errors.NoRespondersError, nats.js.errors.NoStreamResponseError) as e:
            # nobody stored the message: the subject is not persistent
            if self.persistent:
                error = MessengerNotPersisted(self.subject)
                self._error_count += 1
                self._last_error = error
                log.error(f"Message {msg['meta']['id']} not stored. {error}")
                if self.raise_on_publish_error:
                    raise error from e
                msg['meta']['tags'].append('error')
                msg['meta']['status'] = str(error)
                return msg
            # Legacy leniency (until 3.0): the message was sent, count it as published
            self._publish_count += 1
            self._last_publish_time = time.monotonic()
            if not self._warned_not_persistent:
                self._warned_not_persistent = True
                log.warning(
                    f"Publishing to subject '{self.subject}' which is not persistent - messages are NOT stored "
                    f"(reported once per publisher). If they should be, configure persistence for the subject "
                    f"and create the publisher with persistent=True to fail loudly; for fire-and-forget "
                    f"messages use get_corepublisher(). This becomes an error in serverish 3.0")
            else:
                log.debug(f"Message {msg['meta']['id']} published to non-persistent subject '{self.subject}'")
        except Exception as e:
            error = translate_nats_error(e, f"Publishing message {msg['meta']['id']} to '{self.subject}' failed")
            self._error_count += 1
            self._last_error = error
            log.error(f"Trying to publish to subject '{self.subject}' failed. "
                      f"Message {msg['meta']['id']} publish error: {error}")
            if self.raise_on_publish_error:
                if error is e:
                    raise e
                raise error from e
            else:
                msg['meta']['tags'].append('error')
                msg['meta']['status'] = str(error)
        return msg

    async def _publish_with_ack_retries(self, bdata: bytes, headers: dict, msg_id: str, **kwargs) -> None:
        """Publishes to JetStream, retrying when the ack does not arrive in time

        A missing ack does not mean the message was not delivered - it may
        well be stored while the ack was processed too late (e.g. starved
        client event loop). Retries are deduplicated server-side via the
        Nats-Msg-Id header for as long as the stream's duplicate window lasts
        (2 minutes unless the stream says otherwise): a retry sequence which
        fits in the window never produces duplicates, a retry sent later can.

        Raises:
            MessengerNotConnected: JetStream is not connected
            MessengerPublishAckTimeout: no ack after all retries
        """
        js = self.connection.js
        if js is None:
            raise MessengerNotConnected(f"Trying to publish to subject '{self.subject}' failed. "
                                        f"JetStream not connected")
        retry = self._ack_retry
        start = time.monotonic()
        attempt = 0
        while True:
            try:
                await js.publish(self.subject, bdata, headers=headers, **kwargs)
                return
            except nats.errors.TimeoutError as e:
                elapsed = time.monotonic() - start
                if retry.keeps_retrying(attempt, elapsed):
                    delay = retry.delay_for(attempt, elapsed)
                    log.warning(f"No JetStream ack for message {msg_id} on '{self.subject}' "
                                f"(attempt {attempt + 1}), retrying"
                                + (f" in {delay:.1f}s" if delay else "")
                                + " - deduplicated by Nats-Msg-Id")
                    if delay:
                        await asyncio.sleep(delay)
                    attempt += 1
                    continue
                raise MessengerPublishAckTimeout(
                    f"No JetStream ack for message {msg_id} on '{self.subject}' "
                    f"after {attempt + 1} attempts. The message MAY have been "
                    f"delivered - a missing ack often means a starved client event loop, "
                    f"not a delivery failure") from e

    @property
    def health_status(self) -> dict:
        """Returns current health status of the publisher for monitoring

        Returns:
            dict with health information:
                - is_open: Whether the publisher is currently open
                - subject: The subject being published to
                - publish_count: Total successful publishes
                - error_count: Total publish errors
                - last_publish_time: Timestamp of last successful publish (monotonic time)
                - last_publish_ago: Seconds since last publish or None
                - last_error: String representation of last error or None
        """
        last_publish_ago = None
        if self._last_publish_time is not None:
            last_publish_ago = time.monotonic() - self._last_publish_time

        return {
            'is_open': self.is_open,
            'subject': self.subject,
            'publish_count': self._publish_count,
            'error_count': self._error_count,
            'last_publish_time': self._last_publish_time,
            'last_publish_ago': last_publish_ago,
            'last_error': str(self._last_error) if self._last_error else None,
        }


def get_publisher(subject, persistent: bool = False, **kwargs) -> MsgPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to
        persistent (bool): require the subject to be persistent - `open()` and `publish()` raise
            `MessengerNotPersisted` if published messages would not be stored (default in serverish 3.0)
        kwargs: additional MsgPublisher arguments, e.g. error_policy (ErrorPolicy),
            raise_on_publish_error, ack_timeout_retries

    Returns:
        Publisher: a publisher for the given subject

    """
    return Messenger.get_publisher(subject, persistent=persistent, **kwargs)
