from __future__ import annotations

import nats.js.errors
from nats.js.kv import KeyValue

from serverish.base import MessengerKvKeyNotFound
from serverish.messenger.messenger import Messenger, MsgDriver
from serverish.messenger.msg_kv import MsgKvDriver, log


class MsgKvStore(MsgKvDriver):
    """A key-value store over a NATS JetStream KV bucket

    Values are full serverish envelopes, so `get` returns the usual
    (data, meta) pair and `put` validates the message against schemas.

    All operations are decorated with `ensure_open`, so the store can be used
    one-shot without explicit open/close, like other messenger drivers.
    """

    @MsgDriver.ensure_open
    async def put(self, key: str, data: dict | None = None, meta: dict | None = None) -> dict:
        """Stores a value for the key, returns the published message

        Args:
            key (str): key to store the value under
            data (dict): message data
            meta (dict): message metadata

        Returns:
            dict: published message, with meta['kv'] carrying bucket/key/revision
        """
        msg, bdata = self.encode_envelope(data, meta)
        revision = await self.kv.put(key, bdata)
        msg['meta']['kv'] = {'bucket': self.bucket, 'key': key, 'revision': revision, 'operation': 'PUT'}
        self.messenger.log_msg_trace(msg.get('data', {}), msg['meta'], f"KV PUT {self.bucket}/{key}")
        return msg

    @MsgDriver.ensure_open
    async def get(self, key: str, revision: int | None = None) -> tuple[dict, dict]:
        """Returns (data, meta) of the latest (or given revision) value for the key

        Args:
            key (str): key to read
            revision (int): specific revision to read, latest if None

        Raises:
            MessengerKvKeyNotFound: key does not exist (or is deleted)
            MessengerKvMalformed: entry is not a serverish envelope
        """
        try:
            entry = await self.kv.get(key, revision=revision)
        except nats.js.errors.KeyNotFoundError as e:
            raise MessengerKvKeyNotFound(f"Key '{key}' not found in KV bucket '{self.bucket}'") from e
        data, meta = self.decode_entry(entry)
        self.messenger.log_msg_trace(data, meta, f"KV GET {self.bucket}/{key}")
        return data, meta

    @MsgDriver.ensure_open
    async def delete(self, key: str) -> None:
        """Places a delete marker for the key and removes previous revisions"""
        await self.kv.delete(key)
        log.debug(f"KV DEL {self.bucket}/{key}")

    @MsgDriver.ensure_open
    async def purge(self, key: str) -> None:
        """Removes the key including all its revisions"""
        await self.kv.purge(key)
        log.debug(f"KV PURGE {self.bucket}/{key}")

    @MsgDriver.ensure_open
    async def keys(self) -> list[str]:
        """Returns list of keys in the bucket, empty list for an empty bucket"""
        try:
            return await self.kv.keys()
        except nats.js.errors.NoKeysError:
            return []

    @MsgDriver.ensure_open
    async def history(self, key: str) -> list[tuple[dict | None, dict]]:
        """Returns the revision history of the key, oldest first

        Delete/purge markers are returned as (None, meta) entries.
        Note: the bucket must be created with history > 1 to keep past revisions.

        Raises:
            MessengerKvKeyNotFound: key has no history in the bucket
        """
        try:
            entries = await self.kv.history(key)
        except nats.js.errors.NoKeysError as e:
            raise MessengerKvKeyNotFound(f"Key '{key}' has no history in KV bucket '{self.bucket}'") from e
        return [self.decode_entry(entry) for entry in entries]

    @MsgDriver.ensure_open
    async def status(self) -> KeyValue.BucketStatus:
        """Returns the status of the underlying KV bucket"""
        return await self.kv.status()


def get_kvstore(bucket: str, **kwargs) -> MsgKvStore:
    """Returns a key-value store for a given KV bucket

    Args:
        bucket (str): KV bucket name
        kwargs: additional driver arguments (e.g. create_bucket, bucket_config)

    Returns:
        MsgKvStore: a key-value store for the given bucket
    """
    return Messenger.get_kvstore(bucket, **kwargs)


async def kv_put(bucket: str, key: str, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
    """Stores a single value in a KV bucket (one-shot)

    Args:
        bucket (str): KV bucket name
        key (str): key to store the value under
        data (dict): message data
        meta (dict): message metadata
        kwargs: additional driver arguments (e.g. create_bucket)

    Returns:
        dict: published message
    """
    return await get_kvstore(bucket, **kwargs).put(key, data, meta)


async def kv_get(bucket: str, key: str, **kwargs) -> tuple[dict, dict]:
    """Reads a single value from a KV bucket (one-shot)

    Args:
        bucket (str): KV bucket name
        key (str): key to read
        kwargs: additional driver arguments

    Returns:
        tuple[dict, dict]: (data, meta) of the stored message
    """
    return await get_kvstore(bucket, **kwargs).get(key)
