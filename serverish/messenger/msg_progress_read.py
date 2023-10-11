from __future__ import annotations

import logging

from serverish.messenger import Messenger
from serverish.messenger.msg_reader import MsgReader
from serverish.messenger.msg_progress_pub import ProgressTask

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgProgressReader(MsgReader):
    """A class for reading tasks progress published by `MsgProgressPublisher`

    Note, that this mechanism, mimicking `rich.Progress`, supports multiple tasks, distinguished by `task_id`.
    If `stop_when_done` is True (default), the reader will finish iteration when it receives a message with `all_done`
    tag meaning all tasks on publisher side are done.
    Otherwise, the reader will continue reading messages until stopped, which is useful for handling permanent UI
    element like progress bar.
    """

    def __init__(self, subject, parent=None, deliver_policy='last', opt_start_time=None, consumer_cfg=None,
                 stop_when_done: bool = True,
                 **kwargs) -> None:
        super().__init__(subject, parent, deliver_policy, opt_start_time, consumer_cfg, **kwargs)
        self.stop_when_done = stop_when_done

    async def open(self) -> None:
        return await super().open()

    async def close(self) -> None:
        return await super().close()

    def __aiter__(self):
        return super().__aiter__()

    async def __anext__(self) -> (ProgressTask, dict):
        data, meta = await super().__anext__()
        assert meta['message_type'] == 'progress'
        if 'all-done' in meta['tags'] and self.stop_when_done:
            self.stop()
        task = ProgressTask.from_dict(data)
        return task, meta


def get_progressreader(subject: str,
                       deliver_policy='last',
                       stop_when_done: bool = True,
                       **kwargs) -> 'MsgProgressReader':
    """Returns a progress reader for a given subject

    Args:
        subject: subject to read from
        deliver_policy: deliver policy, in this context 'last' is most useful for progress tracking
        stop_when_done: whether to stop iteration when all tasks are done

    Returns:
        MsgProgressReader: a progress reader for the given subject
    """

    return Messenger.get_progressreader(subject=subject,
                                        deliver_policy=deliver_policy,
                                        stop_when_done=stop_when_done,
                                        **kwargs)


