from __future__ import annotations

from serverish.messenger.msg_core_pub import MsgCorePub


class MsgCommandPublisher(MsgCorePub):
    """A publisher for fire-and-forget commands over core NATS (no JetStream)

    Wraps :class:`MsgCorePub` with a convenience :meth:`command` method that
    places the command name and its keyword-argument parameters into the
    standard serverish data envelope.

    Usage::

        async with Messenger().context(host, port):
            pub = get_commandpublisher("svc.command.site.tts")
            await pub.open()
            await pub.command("say", text="hello world", priority=1)
            await pub.close()
    """

    async def command(self, name: str, **params) -> dict:
        """Publish a named command with keyword parameters

        The command name is stored in ``data['command']`` and all keyword
        arguments are stored in ``data['params']``.  Standard serverish meta
        fields (``id``, ``ts``, ``sender``, …) are added automatically.

        Args:
            name (str): command name (e.g. ``"say"``, ``"stop"``).
            **params: arbitrary keyword arguments to pass as command parameters.

        Returns:
            dict: the published message.
        """
        data = {'command': name, 'params': params}
        return await self.publish(data=data)


def get_commandpublisher(subject: str) -> 'MsgCommandPublisher':
    """Returns a command publisher for a given subject (core NATS, no JetStream)

    Args:
        subject (str): subject to publish commands to

    Returns:
        MsgCommandPublisher: a command publisher for the given subject

    Usage::

        async with Messenger().context(host, port):
            pub = get_commandpublisher("svc.command.site.tts")
            await pub.open()
            await pub.command("say", text="hello world")
            await pub.close()
    """
    from serverish.messenger import Messenger
    return Messenger.get_commandpublisher(subject=subject)
