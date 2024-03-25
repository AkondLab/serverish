"""
Based on `rich.progress`, in many cases just plugging this class in place of `rich.progress.Progress` works.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from operator import length_hint
from typing import Iterable, Sequence, TypeVar, Optional

import param

from serverish.base import dt_to_array, dt_from_array
from serverish.base.idmanger import IdManager
from serverish.messenger import Messenger
from serverish.messenger.msg_publisher import MsgPublisher

ProgressType = TypeVar("ProgressType")


@dataclass
class ProgressTask:
    id: str
    description: str
    total: Optional[float]
    completed: float
    finished_time: Optional[float] = None # seconds
    start_time: Optional[datetime] = None
    stop_time: Optional[datetime] = None

    @property
    def started(self) -> bool:
        """bool: Check if the task as started."""
        return self.start_time is not None

    @property
    def remaining(self) -> Optional[float]:
        """Optional[float]: Get the number of steps remaining, if a non-None total was set."""
        if self.total is None:
            return None
        return self.total - self.completed

    @property
    def elapsed(self) -> Optional[float]:
        """Optional[float]: Time elapsed since task was started, or ``None`` if the task hasn't started."""
        if self.start_time is None:
            return None
        if self.stop_time is not None:
            return (self.stop_time - self.start_time).total_seconds()
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()

    @property
    def finished(self) -> bool:
        """Check if the task has finished."""
        return self.finished_time is not None

    @property
    def percentage(self) -> float:
        """float: Get progress of task as a percentage. If a None total was set, returns 0"""
        if not self.total:
            return 0.0
        completed = (self.completed / self.total) * 100.0
        completed = min(100.0, max(0.0, completed))
        return completed

    def _reset(self) -> None:
        """Reset progress."""
        self.finished_time = None

    def to_dict(self):
        return {
            'id': self.id,
            'description': self.description,
            'total': self.total,
            'completed': self.completed,
            'finished_time': self.finished_time,  # seconds or None
            'start_time': dt_to_array(self.start_time),
            'stop_time': dt_to_array(self.stop_time),
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            id = data['id'],
            description = data['description'],
            total = data['total'],
            completed = data['completed'],
            finished_time = data['finished_time'],
            start_time = dt_from_array(data['start_time']),
            stop_time = dt_from_array(data['stop_time']),
        )


class MsgProgressPublisher(MsgPublisher):
    """A class for publishing progress messages to a subject

    Use this class if you want to publish progress data to a messenger subject.
    There is no need to open/close publisher nor usage of context manager, it will be done automatically.
    Anyway Messenger should be initialized before usage of this class.

    The class interface, as well as some implementation, are taken or inspired from "rich" library,
    therefore documentation and usage of `rich.Progress` class can be used as a reference.

    Note, that raise_on_publish_error is set to False by default, which differs from the default in MsgPublisher.
    """
    raise_on_publish_error = param.Boolean(default=False)  # override default
    tasks = param.Dict(default={}, doc="Tasks being tracked")

    async def track(
            self,
            sequence: Iterable[ProgressType] | Sequence[ProgressType],
            total: Optional[float] = None,
            task_id: Optional[str] = None,
            description: str = "Working...",
    ) -> Iterable[ProgressType]:

        if total is None:
            total = float(length_hint(sequence)) or None

        if task_id is None:
            task_id = await self.add_task(description, total=total)
        else:
            await self.update(task_id, total=total)

        for value in sequence:
            yield value
            await self.advance(task_id, 1)

    #ok
    async def start_task(self, task_id: str) -> None:
        """Start a task.

        Starts a task (used when calculating elapsed time). You may need to call this manually,
        if you called ``add_task`` with ``start=False``.

        Args:
            task_id (TaskID): ID of task.
        """
        task = self.tasks[task_id]
        if task.start_time is None:
            task.start_time = datetime.now(timezone.utc)
        await self.publish_progress(task_id)

    #ok
    async def stop_task(self, task_id: str) -> None:
        """Stop a task.

        This will freeze the elapsed time on the task.

        Args:
            task_id (TaskID): ID of task.
        """
        task = self.tasks[task_id]
        current_time = datetime.now(timezone.utc)
        if task.start_time is None:
            task.start_time = current_time
        task.stop_time = current_time
        await self.publish_progress(task_id)

    #ok
    async def update(
        self,
        task_id: str,
        *,
        total: Optional[float] = None,
        completed: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
        refresh: bool = True,
    ) -> None:
        """Update information associated with a task.

        Args:
            task_id (TaskID): Task id (returned by add_task).
            total (float, optional): Updates task.total if not None.
            completed (float, optional): Updates task.completed if not None.
            advance (float, optional): Add a value to task.completed if not None.
            description (str, optional): Change task description if not None.
            refresh (bool): Publish updated progress. Default is True.
        """
        task: ProgressTask = self.tasks[task_id]
        completed_start = task.completed

        if total is not None and total != task.total:
            task.total = total
            task._reset()
        if advance is not None:
            task.completed += advance
        if completed is not None:
            task.completed = completed
        if description is not None:
            task.description = description
        update_completed = task.completed - completed_start

        current_time = datetime.now(timezone.utc)
        # _progress = task._progress

        # popleft = _progress.popleft
        # while _progress and _progress[0].timestamp < old_sample_time:
        #     popleft()
        # if update_completed > 0:
        #     _progress.append(ProgressSample(current_time, update_completed))
        if (
            task.total is not None
            and task.completed >= task.total
            and task.finished_time is None
        ):
            task.finished_time = task.elapsed

        if refresh:
            await self.publish_progress(task_id)

    #ok
    async def reset(
        self,
        task_id: str,
        *,
        start: bool = True,
        total: Optional[float] = None,
        completed: int = 0,
        description: Optional[str] = None,
    ) -> None:
        """Reset a task so completed is 0 and the clock is reset.

        Args:
            task_id (TaskID): ID of task.
            start (bool, optional): Start the task after reset. Defaults to True.
            total (float, optional): New total steps in task, or None to use current total. Defaults to None.
            completed (int, optional): Number of steps completed. Defaults to 0.
            description (str, optional): Change task description if not None. Defaults to None.
        """
        current_time = datetime.now(timezone.utc)
        task = self.tasks[task_id]
        task._reset()
        task.start_time = current_time if start else None
        if total is not None:
            task.total = total
        task.completed = completed
        if description is not None:
            task.description = description
        task.finished_time = None
        await self.publish_progress(task_id)

    #ok
    async def advance(self, task_id: str, advance: float = 1) -> None:
        """Advance task by a number of steps.

        Args:
            task_id (str): ID of task.
            advance (float): Number of steps to advance. Default is 1.
        """
        task = self.tasks[task_id]
        task.completed += advance

        if (
            task.total is not None
            and task.completed >= task.total
            and task.finished_time is None
        ):
            task.finished_time = task.elapsed
        await self.publish_progress(task_id)

    async def publish_progress(self, task_id) -> None:
        """Send the progress information to the server."""
        t = self.tasks[task_id]
        data = t.to_dict()
        tags = ['all-done'] if self.all_done else []
        await self.publish(data, meta={'message_type': 'progress', 'tags': tags})

    @property
    def all_done(self) -> bool:
        """Check if all tasks have been completed."""
        if not self.tasks:
            return True
        return all(task.finished for task in self.tasks.values())



    # ok
    async def add_task(
            self,
            description: str,
            start: bool = True,
            total: Optional[float] = 100.0,
            completed: int = 0,
            task_id: Optional[str] = None,
    ) -> str:
        """Add a new 'task' to the Progress display.

        Args:
            description (str): A description of the task.
            start (bool, optional): Start the task immediately (to calculate elapsed time). If set to False,
                you will need to call `start` manually. Defaults to True.
            total (float, optional): Number of total steps in the progress if known.
                Set to None to render a pulsing animation. Defaults to 100.
            completed (int, optional): Number of steps completed so far. Defaults to 0.
            task_id (TaskID, optional): A unique ID for the task. Defaults to None - autogenerate.
                If not unique, will be supplemented by a suffix. Final id will be returned.

        Returns:
            TaskID: An ID you can use when calling `update`.
        """
        prefix = task_id or 'progress'
        task_id = IdManager().get_id(prefix)
        task = ProgressTask(
            id=task_id,
            description=description,
            total=total,
            completed=completed,
        )
        self.tasks[task_id] = task
        if start:
            await self.start_task(task_id)
        else:
            await self.publish_progress(task_id)
        return task_id

    #ok
    def remove_task(self, task_id: str) -> None:
        """Delete a task if it exists.

        Args:
            task_id (TaskID): A task ID.

        """
        del self.tasks[task_id]

    #ok
    @property
    def finished(self) -> bool:
        """Check if all tasks have been completed."""
        if not self.tasks:
            return True
        return all(task.finished for task in self.tasks.values())


def get_progresspublisher(subject) -> MsgProgressPublisher:
    """Returns a progress tracking publisher for a given subject

    Args:
        subject (str): subject to report progress to

    Returns:
        MsgProgressPublisher: a publisher for the given subject

    """
    return Messenger.get_progresspublisher(subject)

