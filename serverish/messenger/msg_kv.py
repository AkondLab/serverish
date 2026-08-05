"""Key-value bucket support for Messenger.

Provides `MsgKvDriver`, the common base for all KV drivers (`MsgKvStore`,
`MsgKvReader`, `MsgKvSubscriber`). NATS JetStream KV buckets are the backend;
values are always full serverish envelopes ``{"data": ..., "meta": ...}``,
so schema validation and message metadata work exactly as for stream messages.

Delete and purge markers are the only entries without an envelope - the KV
protocol stores them as empty tombstones. Drivers surface them as
``(None, meta)`` with ``meta['kv']['operation']`` set to ``'DEL'`` or ``'PURGE'``.
"""
from __future__ import annotations

import logging

import jsonschema
import nats.js.errors
import param
from nats.js.kv import KeyValue

from serverish.base import MessengerKvBucketNotFound, MessengerKvMalformed, dt_ensure_array
from serverish.messenger.messenger import MsgDriver

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgKvDriver(MsgDriver):
    """Base for KV bucket operators

    KV drivers operate on a bucket (and keys within it) instead of a subject.
    The inherited `subject` is set to the underlying ``$KV.<bucket>`` prefix
    for diagnostics only.

    Args:
        bucket (str): KV bucket name
        create_bucket (bool): create the bucket on open() if it does not exist
        bucket_config (dict): `nats.js.api.KeyValueConfig` parameters (e.g. history, ttl)
            used only when the bucket is being created
    """
    bucket = param.String(default=None, allow_None=True, doc="KV bucket name")
    create_bucket = param.Boolean(default=False, doc="Create the bucket on open() if it does not exist")
    bucket_config = param.Dict(default={}, doc="KeyValueConfig parameters used when creating the bucket")

    def __init__(self, **kwargs) -> None:
        self.kv: KeyValue | None = None
        super().__init__(**kwargs)
        if self.subject is None and self.bucket:
            self.subject = f'$KV.{self.bucket}'  # diagnostic only, KV entries are addressed by bucket/key

    async def open(self) -> None:
        try:
            self.kv = await self.connection.ensure_kv_bucket(self.bucket,
                                                             create_if_needed=self.create_bucket,
                                                             **self.bucket_config)
        except nats.js.errors.BucketNotFoundError as e:
            raise MessengerKvBucketNotFound(
                f"KV bucket '{self.bucket}' does not exist, "
                f"create it externally or open the driver with create_bucket=True") from e
        await super().open()

    async def close(self) -> None:
        self.kv = None
        await super().close()

    def encode_envelope(self, data: dict | None, meta: dict | None) -> tuple[dict, bytes]:
        """Creates and validates a serverish envelope, returns (message, encoded bytes)"""
        msg = self.messenger.create_msg(data, meta)
        try:
            self.messenger.msg_validate(msg)
        except jsonschema.ValidationError as e:
            log.error(f"Message {msg['meta']['id']} validation error: {e}")
            raise e
        return msg, self.messenger.encode(msg)

    def decode_entry(self, entry: KeyValue.Entry) -> tuple[dict | None, dict]:
        """Decodes a KV entry into (data, meta)

        Delete/purge markers carry no envelope: data is None and meta contains
        only the 'kv' section.

        Raises:
            MessengerKvMalformed: entry value is not a serverish envelope
        """
        operation = entry.operation or 'PUT'
        kv_meta = {
            'bucket': entry.bucket,
            'key': entry.key,
            'revision': entry.revision,
            'operation': operation,
        }
        if entry.created is not None:
            kv_meta['created'] = dt_ensure_array(entry.created)
        if operation != 'PUT':
            return None, {'kv': kv_meta}
        try:
            msg = self.messenger.decode(entry.value)
        except (ValueError, TypeError) as e:
            raise MessengerKvMalformed(
                f"Entry '{entry.key}'@{entry.revision} in KV bucket '{entry.bucket}' is not "
                f"a serverish envelope, was it written by a non-serverish client?") from e
        if not isinstance(msg, dict) or 'meta' not in msg:
            raise MessengerKvMalformed(
                f"Entry '{entry.key}'@{entry.revision} in KV bucket '{entry.bucket}' carries "
                f"no meta, was it written by a non-serverish client?")
        data = msg.get('data', {})
        meta = msg['meta']
        meta['kv'] = kv_meta
        return data, meta

    def __str__(self):
        return f'{self.name} [{self.bucket}]'
