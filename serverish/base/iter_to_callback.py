import logging
import asyncio
from asyncio import CancelledError
from typing import Callable, TypeVar, Generic
from serverish.base import Task, create_task

log = logging.getLogger(__name__.rsplit('.')[-1])


U = TypeVar("U")  # Iteration element type


class IterToCallback(Generic[U]):
    """A class for subscribing to a Messenger subject and calling a callback function on each message
    You have to derive from this class and another implementing `__aiter__` and `__anext__` methods"""

    async def close(self) -> None:
        log.info('Closing')
        task = None
        try:
            task = self.task
        except AttributeError:
            pass
        await self.stop()
        if task is not None:
            task.cancel()
        return await super().close()

    async def stop(self) -> None:
        """Stops reading messages"""
        try:
            ev = self._stop_event
            ev.set()
        except AttributeError:
            pass

    async def subscribe(self, callback: Callable[[U, dict], bool] | Callable[[U, dict], asyncio.Future]) -> Task:
        """Sets a callback function for each message

        Args:
            callback: a callback function to call on each message, may be asynchronous
            callback is called with two arguments: data-object and metadata dict,
            and should return True to continue reading messages, False to stop
        """
        self._stop_event = asyncio.Event()
        self.callback: Callable[[U, dict], bool] | Callable[[U, dict], asyncio.Future] = callback
        if asyncio.iscoroutinefunction(callback):
            log.debug(f'Subscribing async {callback} to {self}')
            self.task = await create_task(self._task_body(acb = callback), f'NATSASUB.{self.subject}')
        else:
            log.debug(f'Subscribing sync {callback} to {self}')
            self.task = await create_task(self._task_body(scb= callback), f'NATSSSUB.{self.subject}')
        return self.task

    async def _task_body(self,
                         scb: Callable[[dict, dict], bool] | None = None,
                         acb: Callable[[dict, dict], asyncio.Future] | None = None
                         ) -> None:

        log.debug(f"Starting interation task {self}")
        assert scb is not None or acb is not None
        assert not (scb is not None and acb is not None)
        cb = scb or acb
        continue_iteration = True
        async for data, meta in self:
            log.debug(f'Iteration got message {meta}{str(data):20}')
            try:
                if scb is not None:
                    continue_iteration = scb(data, meta)
                else:
                    continue_iteration = await acb(data, meta)
            except CancelledError:
                log.debug(f'Cancelled {self}')
                break
            except Exception as e:
                log.exception(f'Error in callback {cb} for message {meta}{str(data):20}: {e}')
            if not continue_iteration or self._stop_event.is_set() :
                break
        log.debug(f"Exiting sync interation{self}")



