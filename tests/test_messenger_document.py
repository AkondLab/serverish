"""Integration tests for MsgDocumentReader with NATS.

These tests verify the live document functionality with real NATS messaging:
- Initial document read
- Document updates via NATS publish
- Hierarchical updates and subtree access
- Context manager and lifecycle management
- Integration with Manageable tree
"""

import asyncio
import logging

import pytest

from serverish.base.manageable import Manageable
from serverish.messenger import (
    Messenger,
    get_documentreader,
    get_live_document,
    get_singlepublisher,
    single_publish,
)
from tests.test_connection import ci
from tests.test_nats import is_nats_running

log = logging.getLogger(__name__)


# ============================================================================
# Basic Functionality Tests
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_initial_read():
    """Test reading initial document on open."""
    subject = 'test.document.initial_read'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        initial_config = {
            'database': {
                'host': 'localhost',
                'port': 5432
            },
            'app': {
                'name': 'test_app',
                'version': '1.0.0'
            }
        }
        await single_publish(subject, data=initial_config)

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=5.0)
        await reader.open()

        try:
            # Verify document was loaded
            assert reader.document.database.host == 'localhost'
            assert reader.document.database.port == 5432
            assert reader.document.app.name == 'test_app'
            # Use dict-style access for 'version' key to avoid conflict with version property
            assert reader.document.app['version'] == '1.0.0'

            # Verify version tracking (the LiveDocument attribute)
            assert reader.document._version == 1
            assert reader._seq is not None
        finally:
            await reader.close()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_no_initial_document():
    """Test opening reader when no document exists yet."""
    subject = 'test.document.no_initial'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Create and open reader (no document published yet)
        reader = get_documentreader(subject, initial_wait=0.5)
        await reader.open()

        try:
            # Should have empty document
            assert len(reader.document) == 0
            assert reader.document._version == 0
            assert reader._seq is None
        finally:
            await reader.close()


# ============================================================================
# Update Tests
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_updates():
    """Test that document updates when new version is published."""
    subject = 'test.document.updates'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        initial_config = {
            'database': {
                'host': 'localhost',
                'port': 5432
            }
        }
        await single_publish(subject, data=initial_config)

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=2.0)
        await reader.open()

        try:
            # Verify initial state
            assert reader.document.database.host == 'localhost'
            assert reader.document.database.port == 5432
            initial_seq = reader._seq

            # Publish updated document
            updated_config = {
                'database': {
                    'host': 'remotehost',
                    'port': 3306
                }
            }
            await single_publish(subject, data=updated_config)

            # Wait for update to propagate
            await asyncio.sleep(0.5)

            # Verify document was updated
            assert reader.document.database.host == 'remotehost'
            assert reader.document.database.port == 3306
            assert reader._seq > initial_seq
            assert reader.document._version == 2
        finally:
            await reader.close()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_subtree_updates():
    """Test that cached subtrees are updated when parent updates."""
    subject = 'test.document.subtree_updates'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        initial_config = {
            'telescopes': {
                'jk15': {
                    'components': {
                        'ccd': {
                            'width': 1024,
                            'height': 1024
                        }
                    }
                }
            }
        }
        await single_publish(subject, data=initial_config)

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=2.0)
        await reader.open()

        try:
            # Get reference to subtree
            components = reader.document.telescopes.jk15.components
            assert components.ccd.width == 1024
            assert components.ccd.height == 1024

            # Publish updated document with different CCD dimensions
            updated_config = {
                'telescopes': {
                    'jk15': {
                        'components': {
                            'ccd': {
                                'width': 2048,
                                'height': 2048
                            }
                        }
                    }
                }
            }
            await single_publish(subject, data=updated_config)

            # Wait for update to propagate
            await asyncio.sleep(0.5)

            # Subtree should be updated!
            assert components.ccd.width == 2048
            assert components.ccd.height == 2048
        finally:
            await reader.close()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_multiple_updates():
    """Test multiple sequential updates."""
    subject = 'test.document.multiple_updates'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        await single_publish(subject, data={'counter': 0})

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=2.0)
        await reader.open()

        try:
            assert reader.document.counter == 0
            initial_version = reader.document._version

            # Publish multiple updates
            for i in range(1, 6):
                await single_publish(subject, data={'counter': i})
                await asyncio.sleep(0.2)  # Give time for update to propagate

            # Wait a bit more to ensure all updates processed
            await asyncio.sleep(0.5)

            # Should have the latest value
            assert reader.document.counter == 5
            assert reader.document._version > initial_version
        finally:
            await reader.close()


# ============================================================================
# Lifecycle Management Tests
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_context_manager():
    """Test using document reader as context manager."""
    subject = 'test.document.context_manager'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish document
        await single_publish(subject, data={'key': 'value'})

        # Use as context manager
        async with get_documentreader(subject, initial_wait=2.0) as reader:
            assert reader.document.key == 'value'
            assert reader._update_task is not None

        # After exit, task should be cancelled
        assert reader._update_task.done() or reader._update_task.cancelled()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_explicit_close():
    """Test document reader explicit open/close lifecycle."""
    subject = 'test.document.explicit_close'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish document
        config_data = {
            'database': {
                'host': 'localhost',
                'port': 5432
            }
        }
        await single_publish(subject, data=config_data)

        # get_live_document returns LiveDocument, reader is attached
        doc = await get_live_document(subject, wait=2.0)

        try:
            # Should be able to access document
            assert doc.database.host == 'localhost'
            assert doc.database.port == 5432
            # Reader is attached to document
            assert doc._reader is not None
            # Update task should be running
            assert doc._reader._update_task is not None
            assert not doc._reader._update_task.done()
        finally:
            # Explicit close via attached reader
            await doc._reader.close()
            # Reader's update task should be done/cancelled after close
            assert doc._reader._update_task.done() or doc._reader._update_task.cancelled()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_get_live_document_convenience():
    """Test get_live_document convenience function."""
    subject = 'test.document.convenience'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish document
        await single_publish(subject, data={'name': 'test', 'ver': '1.0'})

        # get_live_document returns LiveDocument directly (similar to single_read)
        doc = await get_live_document(subject, wait=2.0)

        try:
            # Can access data directly from document
            assert doc.name == 'test'
            assert doc['ver'] == '1.0'
            # Reader is attached for lifecycle management
            assert doc._reader is not None
        finally:
            # Close via attached reader if needed
            await doc._reader.close()


# ============================================================================
# Callback Tests
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_with_callbacks():
    """Test document callbacks on updates."""
    subject = 'test.document.callbacks'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        initial_config = {
            'database': {
                'host': 'localhost',
                'port': 5432
            }
        }
        await single_publish(subject, data=initial_config)

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=2.0)
        await reader.open()

        try:
            # Track callback invocations
            callback_called = False
            old_value = None
            new_value = None

            async def on_db_change(old, new):
                nonlocal callback_called, old_value, new_value
                callback_called = True
                old_value = old
                new_value = new

            # Register callback on database subtree
            reader.document.database.on_change(on_db_change)

            # Publish update
            updated_config = {
                'database': {
                    'host': 'newhost',
                    'port': 3306
                }
            }
            await single_publish(subject, data=updated_config)

            # Wait for update and callback
            await asyncio.sleep(0.5)

            # Verify callback was called
            assert callback_called
            assert old_value == {'host': 'localhost', 'port': 5432}
            assert new_value == {'host': 'newhost', 'port': 3306}
        finally:
            await reader.close()


# ============================================================================
# Edge Case Tests
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_removed_subtree():
    """Test accessing subtree after it's removed in an update."""
    subject = 'test.document.removed_subtree'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document with multiple telescopes
        initial_config = {
            'telescopes': {
                'jk15': {
                    'components': {
                        'ccd': {'width': 1024}
                    }
                },
                'jk09': {
                    'components': {
                        'ccd': {'width': 512}
                    }
                }
            }
        }
        await single_publish(subject, data=initial_config)

        # Create and open reader
        reader = get_documentreader(subject, initial_wait=2.0)
        await reader.open()

        try:
            # Get reference to jk15 components
            jk15_components = reader.document.telescopes.jk15.components
            assert jk15_components.ccd.width == 1024

            # Publish update without jk15
            updated_config = {
                'telescopes': {
                    'jk09': {
                        'components': {
                            'ccd': {'width': 512}
                        }
                    }
                }
            }
            await single_publish(subject, data=updated_config)

            # Wait for update
            await asyncio.sleep(0.5)

            # Old reference should be empty now
            assert len(jk15_components) == 0

            # Accessing removed keys should raise errors
            with pytest.raises(AttributeError):
                _ = jk15_components.ccd

            with pytest.raises(KeyError):
                _ = jk15_components['ccd']
        finally:
            await reader.close()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_empty_to_populated():
    """Test starting with no document and receiving first update."""
    subject = 'test.document.empty_to_populated'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Create and open reader (no document yet)
        reader = get_documentreader(subject, initial_wait=0.5)
        await reader.open()

        try:
            # Should start empty
            assert len(reader.document) == 0

            # Publish first document
            first_config = {
                'app': {
                    'name': 'myapp',
                    'version': '1.0'
                }
            }
            await single_publish(subject, data=first_config)

            # Wait for update
            await asyncio.sleep(0.5)

            # Document should now be populated
            assert reader.document.app.name == 'myapp'
            assert reader.document.app['version'] == '1.0'
            assert reader.document._version == 1
        finally:
            await reader.close()


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_document_reader_concurrent_readers():
    """Test multiple readers on the same subject."""
    subject = 'test.document.concurrent_readers'

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)

        # Publish initial document
        await single_publish(subject, data={'counter': 0})

        # Create multiple readers
        reader1 = get_documentreader(subject, initial_wait=2.0)
        reader2 = get_documentreader(subject, initial_wait=2.0)

        await reader1.open()
        await reader2.open()

        try:
            # Both should have initial value
            assert reader1.document.counter == 0
            assert reader2.document.counter == 0

            # Publish update
            await single_publish(subject, data={'counter': 42})

            # Wait for updates
            await asyncio.sleep(0.5)

            # Both should see the update
            assert reader1.document.counter == 42
            assert reader2.document.counter == 42
        finally:
            await reader1.close()
            await reader2.close()
