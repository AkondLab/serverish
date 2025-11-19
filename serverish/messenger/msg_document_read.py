"""Document reader for live configuration documents.

This module provides MsgDocumentReader, a specialized MsgReader that
maintains a live document that auto-updates when new versions are published
to NATS.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import param

from serverish.base import create_task
from serverish.messenger.live_document import LiveDocument
from serverish.messenger.msg_reader import MsgReader, MessengerReaderStopped

if TYPE_CHECKING:
    from serverish.base import Task
    from serverish.base.manageable import Manageable

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgDocumentReader(MsgReader):
    """Reads a document from NATS and keeps it updated.

    This class extends MsgReader to provide a live document that automatically
    updates when new versions are published to the NATS subject. It reads the
    initial document on open() and then subscribes to updates in the background.

    The document is exposed as a LiveDocument instance, supporting both
    dict-style and namespace-style access.

    Supports both explicit open/close and async context manager patterns.
    Integrates with Manageable tree for automatic lifecycle management.

    Attributes:
        document: LiveDocument instance containing the current document state
        initial_wait: Timeout (in seconds) for initial document read

    Example:
        # Explicit open/close
        reader = get_documentreader('app.config')
        await reader.open()
        host = reader.document.database.host
        await reader.close()

        # Context manager
        async with get_documentreader('app.config') as reader:
            host = reader.document.database.host

        # With parent for automatic lifecycle management
        class MyService(Manageable):
            async def open(self):
                self.config_reader = await get_live_document('app.config', parent=self)
                # Auto-closes when MyService closes
    """

    initial_wait = param.Number(
        default=5.0,
        bounds=(0, None),
        doc="Timeout in seconds for initial document read on open()"
    )

    def __init__(
        self,
        subject: str,
        parent: Manageable | None = None,
        initial_wait: float = 5.0,
        **kwargs
    ):
        """Initialize a document reader.

        Args:
            subject: NATS subject to read from
            parent: Parent Manageable for lifecycle management
            initial_wait: Timeout for initial document read (seconds)
            **kwargs: Additional MsgReader options
        """
        # Force deliver_policy='last' to always get the most recent document
        kwargs['deliver_policy'] = 'last'

        super().__init__(subject=subject, parent=parent, **kwargs)

        self.initial_wait = initial_wait
        self.document = LiveDocument({})
        self._update_task: Task | None = None
        self._seq: int | None = None

    async def open(self) -> None:
        """Opens connection, reads initial document, starts update loop.

        This method:
        1. Opens the NATS subscription (via super().open())
        2. Reads the initial document (with timeout)
        3. Starts a background task that listens for updates

        The background task is managed by the task manager and will
        automatically be cancelled when this reader is closed or when
        the parent Manageable is closed.

        Raises:
            asyncio.TimeoutError: If no initial document is available within initial_wait
                Note: The error is logged but not raised; continues with empty document
        """
        await super().open()

        # Initial read - wait for first document
        try:
            data, meta = await asyncio.wait_for(
                self.read_next(),
                timeout=self.initial_wait
            )
            await self.document._update(data)
            self._seq = meta['nats']['seq']
            log.info(f"Document loaded from {self.subject}: seq={self._seq}")
        except asyncio.TimeoutError:
            log.warning(
                f"No initial document found on {self.subject} "
                f"after {self.initial_wait}s, starting with empty document"
            )
            # Continue with empty document - updates will populate it later

        # Start background update loop via task manager
        # Task is tracked by TaskManager and can be cancelled via close()
        self._update_task = await create_task(
            self._update_loop(),
            name=f"doc_update_{self.subject}"
        )

    async def _update_loop(self):
        """Background task that listens for document updates.

        This async task runs in the background after open() and continuously
        listens for new document versions published to the NATS subject.

        It handles:
        - Reading new messages as they arrive
        - Deduplication via sequence numbers
        - Propagating updates to the LiveDocument
        - Graceful shutdown on cancellation or stop signal
        """
        try:
            async for data, meta in self:
                new_seq = meta['nats']['seq']

                # Skip duplicates or old versions
                if self._seq and new_seq <= self._seq:
                    log.debug(
                        f"Skipping duplicate/old document: "
                        f"current seq={self._seq}, received seq={new_seq}"
                    )
                    continue

                log.info(
                    f"Document updated on {self.subject}: "
                    f"{self._seq} -> {new_seq}"
                )
                await self.document._update(data)
                self._seq = new_seq

        except MessengerReaderStopped:
            log.info(f"Document reader stopped for {self.subject}")
        except asyncio.CancelledError:
            log.info(f"Document reader cancelled for {self.subject}")
            raise  # Re-raise for task manager
        except Exception as e:
            log.error(
                f"Document update loop error for {self.subject}: {e}",
                exc_info=True
            )

    async def close(self):
        """Stops update loop and closes connection.

        Task manager handles cancellation automatically, but we clean up
        explicitly to ensure proper shutdown order.
        """
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass

        await super().close()

    # Async context manager support

    async def __aenter__(self):
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"MsgDocumentReader(subject='{self.subject}', "
            f"seq={self._seq}, version={self.document._version})"
        )


def get_documentreader(
    subject: str,
    initial_wait: float = 5.0,
    **kwargs
) -> MsgDocumentReader:
    """Returns a document reader for the given subject.

    This is a convenience function that creates a MsgDocumentReader instance.
    The reader must be explicitly opened before use.

    Args:
        subject: NATS subject to read from
        initial_wait: Timeout for initial document read (seconds)
        **kwargs: Additional MsgReader options

    Returns:
        MsgDocumentReader: A document reader (not yet opened)

    Example:
        reader = get_documentreader('app.config')
        await reader.open()
        host = reader.document.database.host
        await reader.close()
    """
    from serverish.messenger import Messenger

    return Messenger.get_documentreader(
        subject=subject,
        initial_wait=initial_wait,
        **kwargs
    )


async def get_live_document(
    subject: str,
    wait: float = 5.0,
    **kwargs
) -> LiveDocument:
    """Get a live document (already opened and auto-updating).

    This is a convenience function similar to single_read() but for live documents.
    It creates and opens a MsgDocumentReader, then returns the LiveDocument itself.
    The reader is attached to the document for lifecycle management (garbage collection).

    The reader's lifecycle is managed by serverish task manager. When the document
    is garbage collected, the reader will be cleaned up automatically.

    Args:
        subject: NATS subject to read from
        wait: Timeout for initial document read (seconds)
        **kwargs: Additional MsgReader options

    Returns:
        LiveDocument: The live document (dict-like, auto-updates)

    Example:
        # Simple usage - similar to single_read()
        config = await get_live_document('app.config')
        host = config.database.host  # Auto-updates when republished!

        # Access the reader if needed (for explicit close)
        config = await get_live_document('app.config')
        # ... use config ...
        await config._reader.close()  # Explicit cleanup if needed
    """
    reader = get_documentreader(subject, initial_wait=wait, **kwargs)
    await reader.open()

    # Attach reader to document for lifecycle management
    reader.document._reader = reader

    return reader.document
