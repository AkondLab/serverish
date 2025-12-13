"""Live document that auto-updates when underlying data changes.

This module provides LiveDocument, a dict-like object that can be updated
asynchronously and propagates changes through a hierarchical tree structure.
"""

import asyncio
import copy
import logging
from collections.abc import Mapping
from typing import Any, Callable, Iterator

log = logging.getLogger(__name__)


class LiveDocument(Mapping):
    """A dict-like object that auto-updates when the underlying document changes.

    Implements collections.abc.Mapping protocol for full dict compatibility.
    Supports both dictionary-style and namespace-style access:
        - doc['database']['host']  # dict-style
        - doc.database.host         # namespace-style

    LiveDocument creates cached subtrees for nested dicts, ensuring that
    when the root document updates, all subtrees also update automatically.

    Attributes:
        _data: The underlying dictionary data (private)
        _parent: Parent LiveDocument (None for root, private)
        _path: Dot-separated path from root (e.g., "database.connection")
        _children: Cached LiveDocument instances for dict values (private)
        _callbacks: List of (callback, deep) tuples for change notifications (private)
        _lock: Async lock for thread-safe updates (private)
        _version: Incremented on each update (access as attribute, not property)
        _reader: Reference to MsgDocumentReader for lifecycle management (private, root only)

    Example:
        >>> doc = LiveDocument({'database': {'host': 'localhost', 'port': 5432}})
        >>> db = doc['database']  # Returns cached LiveDocument
        >>> db.host  # Returns 'localhost'
        >>> await doc._update({'database': {'host': 'newhost', 'port': 5432}})
        >>> db.host  # Returns 'newhost' - subtree was updated!
    """

    def __init__(
        self,
        data: dict,
        parent: 'LiveDocument | None' = None,
        path: str = ''
    ):
        """Initialize a LiveDocument.

        Args:
            data: Dictionary data to wrap
            parent: Parent LiveDocument if this is a subtree
            path: Dot-separated path from root (e.g., "database.connection")
        """
        self._data = data
        self._parent = parent
        self._path = path
        self._children: dict[str, LiveDocument] = {}
        self._callbacks: list[tuple[Callable, bool]] = []
        self._lock = asyncio.Lock()
        self._version = 0
        self._reader = None  # Will be set by get_live_document() for root document

    # Mapping protocol implementation (required methods)

    def __getitem__(self, key: str) -> Any:
        """Get item - returns LiveDocument for dicts, raw value otherwise.

        Args:
            key: Dictionary key to retrieve

        Returns:
            LiveDocument for nested dicts, raw value otherwise

        Raises:
            KeyError: If key does not exist
        """
        value = self._data[key]
        if isinstance(value, dict):
            # Return CACHED LiveDocument - same instance every time
            if key not in self._children:
                new_path = f'{self._path}.{key}' if self._path else key
                self._children[key] = LiveDocument(value, parent=self, path=new_path)
            return self._children[key]
        return value

    def __iter__(self) -> Iterator[str]:
        """Iterate over keys.

        Returns:
            Iterator over dictionary keys
        """
        return iter(self._data)

    def __len__(self) -> int:
        """Number of keys.

        Returns:
            Number of keys in the document
        """
        return len(self._data)

    # Additional dict-like methods (Mapping provides defaults, but we optimize)

    def get(self, key: str, default: Any = None) -> Any:
        """Get with default value.

        Args:
            key: Dictionary key to retrieve
            default: Default value if key not found

        Returns:
            Value for key, or default if not found
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: object) -> bool:
        """Check if key exists.

        Args:
            key: Key to check

        Returns:
            True if key exists in the document
        """
        return key in self._data

    # Namespace-like access

    def __getattr__(self, key: str) -> Any:
        """Enable doc.database.host syntax.

        Args:
            key: Attribute name to retrieve

        Returns:
            Value for the attribute

        Raises:
            AttributeError: If attribute does not exist
        """
        if key.startswith('_'):
            # Private attributes - use normal lookup
            return object.__getattribute__(self, key)
        try:
            return self[key]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{key}'"
            ) from None

    def __dir__(self):
        """Return list of attributes for autocomplete/introspection.

        Returns:
            List of keys plus standard attributes
        """
        # Include both dict keys and standard attributes
        return list(self._data.keys()) + [
            attr for attr in object.__dir__(self)
            if not attr.startswith('_')
        ]

    # Update mechanism

    async def _update(self, new_data: dict):
        """Update the document data and propagate to children.

        This method is called internally when the document changes.
        It updates the local data, propagates changes to all cached
        children, and triggers any registered callbacks.

        Args:
            new_data: New dictionary data to replace current data
        """
        async with self._lock:
            old_data = copy.deepcopy(self._data)
            self._data = copy.deepcopy(new_data)
            self._version += 1

            # Update all cached children recursively
            for key, child in list(self._children.items()):
                if key in new_data and isinstance(new_data[key], dict):
                    # Key still exists and is still a dict - update it
                    await child._update(new_data[key])
                elif key in new_data:
                    # Key exists but is no longer a dict - remove from children
                    # It will be recreated as needed
                    del self._children[key]
                else:
                    # Key was removed - update child to empty dict
                    await child._update({})

            # Trigger callbacks
            await self._trigger_callbacks(old_data, new_data)

    # Callback support

    def on_change(
        self,
        callback: Callable[[dict, dict], Any],
        deep: bool = True
    ) -> Callable:
        """Register callback for changes in this subtree.

        Args:
            callback: Async function(old_value, new_value) -> None
            deep: If True, trigger on any nested change; if False, only direct changes

        Returns:
            The callback (allows use as decorator)

        Example:
            >>> async def on_db_change(old, new):
            ...     print(f"Database config changed: {old} -> {new}")
            >>> doc.database.on_change(on_db_change)

            Or as a decorator:
            >>> @doc.database.on_change
            ... async def on_db_change(old, new):
            ...     print(f"Database config changed: {old} -> {new}")
        """
        self._callbacks.append((callback, deep))
        return callback

    def remove_callback(self, callback: Callable):
        """Remove a registered callback.

        Args:
            callback: The callback function to remove
        """
        self._callbacks = [
            (cb, deep) for cb, deep in self._callbacks
            if cb != callback
        ]

    async def _trigger_callbacks(self, old_data: dict, new_data: dict):
        """Trigger all registered callbacks.

        Args:
            old_data: Previous data value
            new_data: New data value
        """
        for callback, deep in self._callbacks:
            try:
                # Check if callback is async
                if asyncio.iscoroutinefunction(callback):
                    await callback(old_data, new_data)
                else:
                    callback(old_data, new_data)
            except Exception as e:
                log.error(
                    f"Callback error in {self._path or 'root'}: {e}",
                    exc_info=True
                )

    # Utility methods

    def to_dict(self) -> dict:
        """Convert to a plain dictionary (deep copy).

        Returns:
            Deep copy of the underlying data
        """
        return copy.deepcopy(self._data)

    def snapshot(self) -> 'LiveDocument':
        """Create a point-in-time snapshot (not live).

        Returns:
            New LiveDocument with copied data, no parent/children links
        """
        return LiveDocument(copy.deepcopy(self._data))


    # Representation

    def __repr__(self) -> str:
        """String representation.

        Returns:
            String representation of the document
        """
        if self._path:
            return f"LiveDocument(path='{self._path}', data={self._data!r})"
        return f"LiveDocument({self._data!r})"

    def __str__(self) -> str:
        """User-friendly string representation.

        Returns:
            String representation of the underlying data
        """
        return str(self._data)

    # python
    def _plain_value(self, value):
        """Return a plain deep-copied value suitable for serialization."""
        if isinstance(value, LiveDocument):
            return value.to_dict()
        if isinstance(value, dict):
            return copy.deepcopy(value)
        # for simple immutable values deepcopy is fine; for other objects leave as-is
        try:
            return copy.deepcopy(value)
        except Exception:
            return value

    def items(self):
        """Yield (key, plain_value) pairs suitable for dict(...) / JSON dumps.

        This does a deep conversion of any nested LiveDocument/dict but does not
        change __getitem__ behaviour (so ld[key] still returns LiveDocument).
        """
        for key in self._data:
            # if we have a cached child LiveDocument, use its snapshot/plain dict
            child = self._children.get(key)
            if child is not None:
                yield key, child.to_dict()
            else:
                yield key, self._plain_value(self._data[key])

    def values(self):
        """Yield plain values (converted) for each key."""
        for _, v in self.items():
            yield v

    def keys(self):
        """Return an iterator of keys (same as before)."""
        return iter(self._data.keys())



