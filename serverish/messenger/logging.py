import asyncio
import logging

from serverish.messenger import get_journalpublisher


class NatsJournalLoggingHandler(logging.Handler):
    def __init__(self, subject, **kwargs):
        super().__init__(**kwargs)
        self._loop = None  # lazy init
        # self.thread_id = threading.current_thread().ident
        self.publisher = get_journalpublisher(subject)

    @property
    def loop(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    def emit(self, record):
        message = self.format(record)
        # dict_msg = {
        #     'message': message,
        #     'loop_thread': self.thread_id,
        #     'current_thread': threading.current_thread().ident
        # }
        coroutine = self.publisher.log(record.levelno, message)
        asyncio.run_coroutine_threadsafe(coroutine, self.loop)

        # if asyncio.get_event_loop().is_running():
        #     # Jesteśmy w wątku z uruchomioną pętlą asyncio
        #     # dict_msg['loop'] = 'running'
        #     coroutine = self.publisher.log(record.levelno, message)
        #     # asyncio.ensure_future(coroutine)
        #     # self.loop.call_soon_threadsafe(asyncio.ensure_future, coroutine)
        #     # self.loop.call_soon_threadsafe(send_message, {'message': message, 'loop': 'running'})
        #     asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        # else:
        #     # Jesteśmy poza wątkiem pętli asyncio
        #     dict_msg['loop'] = 'not running'
        #     coroutine = send_message(dict_msg)
        #     self.loop.call_soon_threadsafe(asyncio.ensure_future, coroutine)
