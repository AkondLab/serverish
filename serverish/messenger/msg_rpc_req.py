from __future__ import annotations

import asyncio
from asyncio import CancelledError

from nats.errors import NoRespondersError, TimeoutError

from serverish.base import MessengerRequestNoResponders, MessengerRequestCanceled, MessengerRequestNoResultYet, \
    MessengerRequestTimeout, MessengerRequestJetStreamSubject
from serverish.base import Task, create_task
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver


class MsgRpcRequester(MsgDriver):
    """A class for publishing messages to a Messenger subject and waiting for a response

    This class works like `MsgPublisher`, but allows to wait for a response message

    Usage:
        requester = MsgRpcRequester(subject='subject')
        try:
            response = await requester.request(data={'foo': 'bar'})
        except MessengerRequestNoResponders:
            print('No responders for request')
        else:
            print(response)
    """

    @MsgDriver.ensure_open
    async def request(self,
                      data: dict | None = None, meta: dict | None = None,
                      timeout: float | None = None) -> (dict, dict):
        """Publishes a message and waits for a response

        Args:
            data (dict): message data
            meta (dict): message metadata
            timeout (float): time to wait for a response, None to wait forever

        Returns:
            tuple: response message data and metadata

        Raises:
            MessengerRequestNoResponders: if no responders were found for the request
            MessengerRequestTimeout: if no response was received in time
        """
        from nats.aio.client import Client as NATS

        nats: NATS = self.connection.nc
        msg = self.messenger.create_msg(data, meta=meta)
        bmsg = self.messenger.encode(msg)
        try:
            resp = await nats.request(self.subject, payload=bmsg, timeout=timeout)
        except NoRespondersError as e:
            raise MessengerRequestNoResponders(e)
        except TimeoutError as e:
            raise MessengerRequestTimeout(e)
        rdata, rmeta = self.messenger.unpack_nats_msg(resp)
        if rmeta is None:
            raise MessengerRequestJetStreamSubject(self.subject)
        return rdata, rmeta



    async def request_nowait(self, data: dict | None = None, meta: dict | None = None) -> Task:
        """Publishes a request message and returns immediately

        Use `result` to wait for a response

        Args:
            data (dict): message data
            meta (dict): message metadata
        """
        t = await create_task(self.request(data, meta=meta), f'NATSREQ.{self.subject}')
        return t

    async def result(self, t:Task, wait: float | None = None) -> (dict, dict):
        """Waits for a response to a request made be request_nowait

        Args:
            t (Task): task returned by `request_nowait`
            wait (float): time to wait for a response, None to check for response without waiting

        Returns:
            tuple: (data, meta) - response message data and metadata

        Raises:
            MessengerRequestNoResultYet: if no response was received yet (or in wait time);
                                         the request is not canceled, so you can call `result` again
            MessengerRequestCanceled: if request was canceled
            MessengerRequestNoResponders: if no responders were found for the request
        """

        try:
            r = t.task.result()
        except asyncio.CancelledError:
            raise MessengerRequestCanceled()
        except asyncio.InvalidStateError:
            r = None

        if r is None:
            if wait is None:
                raise MessengerRequestNoResultYet()
            try:
                r = await asyncio.wait_for(asyncio.shield(t.task), wait)
            except asyncio.TimeoutError:
                raise MessengerRequestNoResultYet()
            except asyncio.CancelledError:
                raise MessengerRequestCanceled()

        data, meta = self.messenger.unpack_nats_msg(r)
        return data, meta

    def cancel(self, t:Task) -> None:
        """Cancels a request  made be request_nowait

        Args:
            t (Task): task returned by `request_nowait`
        """
        t.cancel()
        # try:
        #     await t.task
        # except asyncio.CancelledError:
        #     pass

    async def open(self) -> None:
        """Opens the requester"""

        # check if the subject is a JetStream subject which is not supported by request/response mechanism
        try:
            js = self.connection.js
            stream = await js.find_stream_name_by_subject(self.subject)
        except:
            pass
        else:
            raise MessengerRequestJetStreamSubject(self.subject)  # stream for subject found
        await super().open()


def get_rpcrequester(subject) -> MsgRpcRequester:
    """Returns a RPC requester for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        MsgRpcRequester: a publisher for the given subject

    """
    return Messenger.get_rpcrequester(subject)


async def request(subject,
                  data: dict | None = None, meta: dict | None = None,
                  timeout: float | None = None) -> (dict, dict):
    """Makes a single request and waits for a response

    Args:
        subject (str): subject to publish to
        data (dict): message data
        meta (dict): message metadata
        timeout (float): time to wait for a response, None to wait forever

    Returns:
        tuple: response message data and metadata
    """
    pub = get_rpcrequester(subject)
    return await pub.request(data, meta=meta, timeout=timeout)
