from __future__ import annotations

import param
from nats.js.kv import KeyValue

from serverish.base import MessengerReaderStopped
from serverish.connection.nats_errors import nats_errors_translated
from serverish.messenger.messenger import Messenger
from serverish.messenger.msg_kv import MsgKvDriver, log


class MsgKvReader(MsgKvDriver):
    """An async-iterator over changes of KV keys

    Iteration yields (data, meta) pairs like `MsgReader`; the key, revision and
    operation are available in `meta['kv']`. On open, current values of matching
    keys are delivered first, then live updates as they happen.

    Delete/purge markers are yielded as (None, meta) with
    meta['kv']['operation'] set to 'DEL'/'PURGE' (unless ignore_deletes is set).

    With ``nowait=True`` the iteration finishes once the values existing at
    open() have been delivered - a snapshot of the current state of the bucket,
    the same meaning `nowait` has on `MsgReader`.

    Usage:
        reader = get_kvreader('bucket', key='telescope.>')
        await reader.open()
        async for data, meta in reader:
            print(meta['kv']['key'], data)
    """
    key = param.String(default='>', doc="Key or wildcard pattern to watch")
    include_history = param.Boolean(default=False, doc="Deliver historical revisions of the keys on open")
    ignore_deletes = param.Boolean(default=False, doc="Skip delete/purge markers")
    nowait = param.Boolean(default=False, doc="Finish the iteration once the values existing at open() "
                                              "have been delivered, instead of waiting for further changes")

    def __init__(self, **kwargs) -> None:
        self._watcher: KeyValue.KeyWatcher | None = None
        self._exhausted: bool = False  # nowait: the current state has been delivered
        super().__init__(**kwargs)

    async def open(self) -> None:
        await super().open()
        self._exhausted = False
        with nats_errors_translated(f"Watching KV {self.bucket}/{self.key} failed"):
            self._watcher = await self.kv.watch(self.key,
                                                include_history=self.include_history,
                                                ignore_deletes=self.ignore_deletes)

    async def close(self) -> None:
        await self._stop_watcher()
        await super().close()

    async def _stop_watcher(self) -> None:
        if self._watcher is not None:
            try:
                await self._watcher.stop()
            except Exception as e:
                log.debug(f"Error stopping KV watcher for {self}: {e}")
            self._watcher = None

    def __aiter__(self):
        return self

    async def __anext__(self) -> tuple[dict | None, dict]:
        if self._exhausted:
            raise StopAsyncIteration  # stays finished until the next open()
        if self._watcher is None:
            raise MessengerReaderStopped(f"KV reader {self} is not open")
        while True:
            with nats_errors_translated(f"Watching KV {self.bucket}/{self.key} failed"):
                entry = await self._watcher.__anext__()  # raises StopAsyncIteration when watcher is stopped
            if entry is None:
                # nats-py sends a None marker once the initial replay is done, it is not a value
                if self.nowait:
                    # the current state has been delivered, that is all we were asked for:
                    # no more updates may arrive, now or on a later iteration
                    self._exhausted = True
                    await self._stop_watcher()
                    raise StopAsyncIteration
                continue
            return self.decode_entry(entry)

    def __str__(self):
        return f'{self.name} [{self.bucket}/{self.key}]'


def get_kvreader(bucket: str, key: str = '>', **kwargs) -> MsgKvReader:
    """Returns a KV reader for a given bucket and key pattern

    Args:
        bucket (str): KV bucket name
        key (str): key or wildcard pattern to watch
        kwargs: additional driver arguments (e.g. include_history, ignore_deletes, nowait)

    Returns:
        MsgKvReader: a KV reader for the given bucket
    """
    return Messenger.get_kvreader(bucket, key=key, **kwargs)
