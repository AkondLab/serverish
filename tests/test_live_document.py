"""Tests for LiveDocument functionality.

This module tests LiveDocument independently of MsgDocumentReader,
focusing on:
- Mapping protocol compliance
- Dict-style and namespace-style access
- Hierarchical structure with cached subtrees
- Update propagation through the tree
- Callback functionality
"""

import asyncio
import pytest

from serverish.messenger.live_document import LiveDocument


# ============================================================================
# Dict Protocol Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_basic_dict_access():
    """Test basic dictionary-style access."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    assert doc['name'] == 'test'
    assert doc['count'] == 42


@pytest.mark.asyncio
async def test_livedocument_get_method():
    """Test get() method with defaults."""
    doc = LiveDocument({'name': 'test'})

    assert doc.get('name') == 'test'
    assert doc.get('missing') is None
    assert doc.get('missing', 'default') == 'default'


@pytest.mark.asyncio
async def test_livedocument_contains():
    """Test 'in' operator."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    assert 'name' in doc
    assert 'count' in doc
    assert 'missing' not in doc


@pytest.mark.asyncio
async def test_livedocument_len():
    """Test len() function."""
    doc = LiveDocument({'a': 1, 'b': 2, 'c': 3})

    assert len(doc) == 3


@pytest.mark.asyncio
async def test_livedocument_keys():
    """Test keys() method."""
    doc = LiveDocument({'a': 1, 'b': 2, 'c': 3})

    keys = list(doc.keys())
    assert set(keys) == {'a', 'b', 'c'}


@pytest.mark.asyncio
async def test_livedocument_values():
    """Test values() method."""
    doc = LiveDocument({'a': 1, 'b': 2, 'c': 3})

    values = list(doc.values())
    assert set(values) == {1, 2, 3}


@pytest.mark.asyncio
async def test_livedocument_items():
    """Test items() method."""
    doc = LiveDocument({'a': 1, 'b': 2, 'c': 3})

    items = list(doc.items())
    assert set(items) == {('a', 1), ('b', 2), ('c', 3)}


@pytest.mark.asyncio
async def test_livedocument_iteration():
    """Test iteration over keys."""
    doc = LiveDocument({'a': 1, 'b': 2, 'c': 3})

    keys = [k for k in doc]
    assert set(keys) == {'a', 'b', 'c'}


@pytest.mark.asyncio
async def test_livedocument_keyerror():
    """Test KeyError for missing keys."""
    doc = LiveDocument({'name': 'test'})

    with pytest.raises(KeyError):
        _ = doc['missing']


# ============================================================================
# Namespace-Style Access Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_namespace_access():
    """Test namespace-style attribute access."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    assert doc.name == 'test'
    assert doc.count == 42


@pytest.mark.asyncio
async def test_livedocument_namespace_attributeerror():
    """Test AttributeError for missing attributes."""
    doc = LiveDocument({'name': 'test'})

    with pytest.raises(AttributeError):
        _ = doc.missing


@pytest.mark.asyncio
async def test_livedocument_namespace_nested():
    """Test namespace-style access on nested dicts."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    assert doc.database.host == 'localhost'
    assert doc.database.port == 5432


# ============================================================================
# Hierarchical Structure Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_nested_dict_returns_livedocument():
    """Test that accessing nested dict returns LiveDocument."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    db = doc['database']
    assert isinstance(db, LiveDocument)
    assert db['host'] == 'localhost'


@pytest.mark.asyncio
async def test_livedocument_cached_subtrees():
    """Test that subtrees are cached (same instance returned)."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    db1 = doc['database']
    db2 = doc['database']

    # Should be the exact same instance
    assert db1 is db2


@pytest.mark.asyncio
async def test_livedocument_subtree_path():
    """Test that subtrees have correct paths."""
    doc = LiveDocument({
        'database': {
            'connection': {
                'pool': {
                    'size': 10
                }
            }
        }
    })

    assert doc._path == ''
    assert doc.database._path == 'database'
    assert doc.database.connection._path == 'database.connection'
    assert doc.database.connection.pool._path == 'database.connection.pool'


@pytest.mark.asyncio
async def test_livedocument_mixed_access():
    """Test mixing dict-style and namespace-style access."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    # Mix styles
    assert doc['database'].host == 'localhost'
    assert doc.database['port'] == 5432


# ============================================================================
# Update and Propagation Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_simple_update():
    """Test simple update of root document."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    assert doc['name'] == 'test'
    assert doc['count'] == 42

    await doc._update({'name': 'updated', 'count': 100})

    assert doc['name'] == 'updated'
    assert doc['count'] == 100


@pytest.mark.asyncio
async def test_livedocument_update_increments_version():
    """Test that updates increment version number."""
    doc = LiveDocument({'name': 'test'})

    assert doc._version == 0

    await doc._update({'name': 'updated1'})
    assert doc._version == 1

    await doc._update({'name': 'updated2'})
    assert doc._version == 2


@pytest.mark.asyncio
async def test_livedocument_update_propagates_to_subtrees():
    """Test that updates propagate to cached subtrees."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    # Get subtree before update
    db = doc['database']
    assert db.host == 'localhost'
    assert db.port == 5432

    # Update root document
    await doc._update({
        'database': {
            'host': 'newhost',
            'port': 3306
        }
    })

    # Subtree should be updated!
    assert db.host == 'newhost'
    assert db.port == 3306


@pytest.mark.asyncio
async def test_livedocument_deep_subtree_update_propagation():
    """Test that updates propagate through deep hierarchies."""
    doc = LiveDocument({
        'app': {
            'database': {
                'connection': {
                    'host': 'localhost',
                    'port': 5432
                }
            }
        }
    })

    # Get deep subtree
    conn = doc.app.database.connection
    assert conn.host == 'localhost'
    assert conn.port == 5432

    # Update root
    await doc._update({
        'app': {
            'database': {
                'connection': {
                    'host': 'remotehost',
                    'port': 3306
                }
            }
        }
    })

    # Deep subtree should be updated
    assert conn.host == 'remotehost'
    assert conn.port == 3306


@pytest.mark.asyncio
async def test_livedocument_update_with_removed_keys():
    """Test update when keys are removed."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432,
            'user': 'admin'
        }
    })

    db = doc['database']
    assert db.host == 'localhost'
    assert db.user == 'admin'

    # Update with fewer keys
    await doc._update({
        'database': {
            'host': 'newhost',
            'port': 5432
        }
    })

    # Host should be updated
    assert db.host == 'newhost'
    # User should be gone
    assert 'user' not in db
    with pytest.raises(KeyError):
        _ = db['user']


@pytest.mark.asyncio
async def test_livedocument_update_with_new_keys():
    """Test update when new keys are added."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost'
        }
    })

    db = doc['database']
    assert 'port' not in db

    # Update with new key
    await doc._update({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    # New key should be accessible
    assert db.port == 5432


@pytest.mark.asyncio
async def test_livedocument_update_dict_to_value():
    """Test update when a dict becomes a simple value."""
    doc = LiveDocument({
        'config': {
            'nested': {
                'value': 42
            }
        }
    })

    # Access nested as LiveDocument
    nested = doc.config.nested
    assert isinstance(nested, LiveDocument)
    assert nested.value == 42

    # Update so 'nested' is no longer a dict
    await doc._update({
        'config': {
            'nested': 'simple_string'
        }
    })

    # Now 'nested' should be a simple value
    assert doc.config.nested == 'simple_string'
    assert not isinstance(doc.config.nested, LiveDocument)


@pytest.mark.asyncio
async def test_livedocument_update_value_to_dict():
    """Test update when a simple value becomes a dict."""
    doc = LiveDocument({
        'config': {
            'setting': 'simple_value'
        }
    })

    assert doc.config.setting == 'simple_value'

    # Update so 'setting' becomes a dict
    await doc._update({
        'config': {
            'setting': {
                'host': 'localhost',
                'port': 8080
            }
        }
    })

    # Now 'setting' should be a LiveDocument
    assert isinstance(doc.config.setting, LiveDocument)
    assert doc.config.setting.host == 'localhost'
    assert doc.config.setting.port == 8080


@pytest.mark.asyncio
async def test_livedocument_update_isolated_subtrees():
    """Test that updates to one subtree don't affect others."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        },
        'cache': {
            'host': 'redis',
            'port': 6379
        }
    })

    db = doc.database
    cache = doc.cache

    # Update only database
    await doc._update({
        'database': {
            'host': 'newhost',
            'port': 3306
        },
        'cache': {
            'host': 'redis',
            'port': 6379
        }
    })

    # Database should be updated
    assert db.host == 'newhost'
    assert db.port == 3306

    # Cache should be unchanged
    assert cache.host == 'redis'
    assert cache.port == 6379


# ============================================================================
# Callback Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_callback_on_update():
    """Test that callbacks are triggered on update."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    callback_called = False
    old_value = None
    new_value = None

    async def callback(old, new):
        nonlocal callback_called, old_value, new_value
        callback_called = True
        old_value = old
        new_value = new

    doc.on_change(callback)

    await doc._update({'name': 'updated', 'count': 100})

    assert callback_called
    assert old_value == {'name': 'test', 'count': 42}
    assert new_value == {'name': 'updated', 'count': 100}


@pytest.mark.asyncio
async def test_livedocument_multiple_callbacks():
    """Test that multiple callbacks are all triggered."""
    doc = LiveDocument({'name': 'test'})

    callback1_called = False
    callback2_called = False

    async def callback1(old, new):
        nonlocal callback1_called
        callback1_called = True

    async def callback2(old, new):
        nonlocal callback2_called
        callback2_called = True

    doc.on_change(callback1)
    doc.on_change(callback2)

    await doc._update({'name': 'updated'})

    assert callback1_called
    assert callback2_called


@pytest.mark.asyncio
async def test_livedocument_callback_on_subtree():
    """Test callbacks on subtrees."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    callback_called = False
    old_value = None
    new_value = None

    async def callback(old, new):
        nonlocal callback_called, old_value, new_value
        callback_called = True
        old_value = old
        new_value = new

    # Register callback on subtree
    doc.database.on_change(callback)

    # Update root document
    await doc._update({
        'database': {
            'host': 'newhost',
            'port': 3306
        }
    })

    # Subtree callback should be triggered
    assert callback_called
    assert old_value == {'host': 'localhost', 'port': 5432}
    assert new_value == {'host': 'newhost', 'port': 3306}


@pytest.mark.asyncio
async def test_livedocument_sync_callback():
    """Test that synchronous callbacks work too."""
    doc = LiveDocument({'name': 'test'})

    callback_called = False

    def sync_callback(old, new):
        nonlocal callback_called
        callback_called = True

    doc.on_change(sync_callback)

    await doc._update({'name': 'updated'})

    assert callback_called


@pytest.mark.asyncio
async def test_livedocument_callback_exception_handling():
    """Test that callback exceptions don't break updates."""
    doc = LiveDocument({'name': 'test'})

    callback2_called = False

    async def bad_callback(old, new):
        raise ValueError("Intentional error")

    async def good_callback(old, new):
        nonlocal callback2_called
        callback2_called = True

    doc.on_change(bad_callback)
    doc.on_change(good_callback)

    # Update should succeed despite bad callback
    await doc._update({'name': 'updated'})

    # Document should be updated
    assert doc.name == 'updated'
    # Good callback should still run
    assert callback2_called


@pytest.mark.asyncio
async def test_livedocument_remove_callback():
    """Test removing callbacks."""
    doc = LiveDocument({'name': 'test'})

    callback_called = False

    async def callback(old, new):
        nonlocal callback_called
        callback_called = True

    doc.on_change(callback)
    doc.remove_callback(callback)

    await doc._update({'name': 'updated'})

    # Callback should not be called
    assert not callback_called


@pytest.mark.asyncio
async def test_livedocument_callback_decorator_style():
    """Test using on_change as a decorator."""
    doc = LiveDocument({'name': 'test'})

    callback_called = False

    @doc.on_change
    async def my_callback(old, new):
        nonlocal callback_called
        callback_called = True

    await doc._update({'name': 'updated'})

    assert callback_called


# ============================================================================
# Utility Method Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_to_dict():
    """Test to_dict() returns a plain dict copy."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    plain_dict = doc.to_dict()

    # Should be a plain dict
    assert isinstance(plain_dict, dict)
    assert plain_dict == {
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    }

    # Should be a copy (modifying it doesn't affect doc)
    plain_dict['database']['host'] = 'modified'
    assert doc.database.host == 'localhost'


@pytest.mark.asyncio
async def test_livedocument_snapshot():
    """Test snapshot() creates independent copy."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    snapshot = doc.snapshot()

    # Should be a LiveDocument
    assert isinstance(snapshot, LiveDocument)
    assert snapshot.name == 'test'
    assert snapshot.count == 42

    # Should be independent (updates to original don't affect snapshot)
    await doc._update({'name': 'updated', 'count': 100})

    assert doc.name == 'updated'
    assert snapshot.name == 'test'


@pytest.mark.asyncio
async def test_livedocument_version_attribute():
    """Test _version attribute access."""
    doc = LiveDocument({'name': 'test'})

    assert doc._version == 0

    await doc._update({'name': 'v1'})
    assert doc._version == 1

    await doc._update({'name': 'v2'})
    assert doc._version == 2


@pytest.mark.asyncio
async def test_livedocument_path_attribute():
    """Test _path attribute access."""
    doc = LiveDocument({
        'database': {
            'connection': {
                'pool': {
                    'size': 10
                }
            }
        }
    })

    assert doc._path == ''
    assert doc.database._path == 'database'
    assert doc.database.connection._path == 'database.connection'
    assert doc.database.connection.pool._path == 'database.connection.pool'


# ============================================================================
# Representation Tests
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_repr():
    """Test __repr__ output."""
    doc = LiveDocument({'name': 'test'})

    repr_str = repr(doc)
    assert 'LiveDocument' in repr_str
    assert 'name' in repr_str
    assert 'test' in repr_str


@pytest.mark.asyncio
async def test_livedocument_repr_with_path():
    """Test __repr__ includes path for subtrees."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost'
        }
    })

    db = doc.database
    repr_str = repr(db)
    assert 'LiveDocument' in repr_str
    assert 'database' in repr_str


@pytest.mark.asyncio
async def test_livedocument_str():
    """Test __str__ output."""
    doc = LiveDocument({'name': 'test', 'count': 42})

    str_output = str(doc)
    assert 'name' in str_output
    assert 'test' in str_output
    assert '42' in str_output


# ============================================================================
# Edge Cases and Complex Scenarios
# ============================================================================

@pytest.mark.asyncio
async def test_livedocument_empty_dict():
    """Test with empty dictionary."""
    doc = LiveDocument({})

    assert len(doc) == 0
    assert list(doc.keys()) == []


@pytest.mark.asyncio
async def test_livedocument_deeply_nested():
    """Test with deeply nested structure."""
    doc = LiveDocument({
        'level1': {
            'level2': {
                'level3': {
                    'level4': {
                        'level5': {
                            'value': 'deep'
                        }
                    }
                }
            }
        }
    })

    assert doc.level1.level2.level3.level4.level5.value == 'deep'


@pytest.mark.asyncio
async def test_livedocument_mixed_types():
    """Test with mixed value types."""
    doc = LiveDocument({
        'string': 'hello',
        'number': 42,
        'float': 3.14,
        'bool': True,
        'none': None,
        'list': [1, 2, 3],
        'nested': {
            'key': 'value'
        }
    })

    assert doc.string == 'hello'
    assert doc.number == 42
    assert doc.float == 3.14
    assert doc.bool is True
    assert doc.none is None
    assert doc.list == [1, 2, 3]
    assert isinstance(doc.nested, LiveDocument)


@pytest.mark.asyncio
async def test_livedocument_concurrent_updates():
    """Test concurrent updates (thread safety)."""
    doc = LiveDocument({'counter': 0})

    async def update_task(i):
        await doc._update({'counter': i})

    # Run multiple updates concurrently
    tasks = [update_task(i) for i in range(10)]
    await asyncio.gather(*tasks)

    # Should have one of the values
    assert doc.counter in range(10)
    # Version should be 10 (all updates completed)
    assert doc._version == 10


@pytest.mark.asyncio
async def test_livedocument_callback_with_async_operations():
    """Test callbacks that perform async operations."""
    doc = LiveDocument({'name': 'test'})

    results = []

    async def callback_with_sleep(old, new):
        await asyncio.sleep(0.01)
        results.append(new['name'])

    doc.on_change(callback_with_sleep)

    await doc._update({'name': 'updated'})

    assert 'updated' in results


@pytest.mark.asyncio
async def test_livedocument_dir():
    """Test __dir__ for autocomplete."""
    doc = LiveDocument({
        'database': {
            'host': 'localhost'
        },
        'cache': {
            'host': 'redis'
        }
    })

    dir_result = dir(doc)

    # Should include dict keys
    assert 'database' in dir_result
    assert 'cache' in dir_result
    # Should include standard methods
    assert 'get' in dir_result
    assert 'keys' in dir_result


@pytest.mark.asyncio
async def test_livedocument_no_conflict_with_version_path_keys():
    """Test that 'version' and 'path' can be used as data keys without conflict."""
    doc = LiveDocument({
        'app': {
            'name': 'myapp',
            'version': '1.2.3',  # User's version key
            'path': '/usr/local/bin'  # User's path key
        },
        'database': {
            'version': '14.5',
            'path': '/var/lib/postgres'
        }
    })

    # Namespace-style access works for user data keys
    assert doc.app.version == '1.2.3'
    assert doc.app.path == '/usr/local/bin'
    assert doc.database.version == '14.5'
    assert doc.database.path == '/var/lib/postgres'

    # Dict-style access also works
    assert doc['app']['version'] == '1.2.3'
    assert doc['app']['path'] == '/usr/local/bin'

    # LiveDocument internal attributes use underscore prefix
    assert doc._version == 0  # Update counter
    assert doc._path == ''  # Hierarchical path from root
    assert doc.app._version == 0
    assert doc.app._path == 'app'

    # Update the document
    await doc._update({
        'app': {
            'name': 'myapp',
            'version': '2.0.0',  # Changed user version
            'path': '/opt/myapp'  # Changed user path
        },
        'database': {
            'version': '15.0',
            'path': '/data/postgres'
        }
    })

    # User data updated
    assert doc.app.version == '2.0.0'
    assert doc.app.path == '/opt/myapp'

    # LiveDocument version counter incremented
    assert doc._version == 1


@pytest.mark.asyncio
async def test_livedocument_access_removed_subtree():
    """Test accessing a subtree reference after it's been removed from parent.

    Scenario:
    1. User gets reference to deep subtree
    2. Uses it successfully
    3. Parent document is updated WITHOUT that subtree
    4. User tries to access the old subtree reference

    Expected: Should raise KeyError (dict-style) or AttributeError (namespace-style)
    """
    # Setup initial document with nested telescopes
    config = LiveDocument({
        'telescopes': {
            'jk15': {
                'components': {
                    'ccd': {
                        'width': 1024,
                        'height': 1024
                    },
                    'mount': {
                        'type': 'equatorial'
                    }
                }
            },
            'jk09': {
                'components': {
                    'ccd': {
                        'width': 512,
                        'height': 512
                    }
                }
            }
        }
    })

    # Step 1: User accesses deep subtree
    sub = config['telescopes']['jk15']['components']

    # Step 2: Uses it successfully
    assert sub.ccd.width == 1024
    assert sub['ccd']['height'] == 1024
    assert sub.mount.type == 'equatorial'

    # Step 3: Document is updated WITHOUT jk15 telescope
    await config._update({
        'telescopes': {
            'jk09': {
                'components': {
                    'ccd': {
                        'width': 512,
                        'height': 512
                    }
                }
            }
        }
    })

    # Step 4: User tries to access the old subtree reference
    # The subtree still exists as an object, but its data has been updated to {}

    # Dict-style access should raise KeyError
    with pytest.raises(KeyError):
        _ = sub['ccd']

    with pytest.raises(KeyError):
        _ = sub['mount']

    # Namespace-style access should raise AttributeError
    with pytest.raises(AttributeError):
        _ = sub.ccd

    with pytest.raises(AttributeError):
        _ = sub.mount

    # The subtree should be empty now
    assert len(sub) == 0
    assert list(sub.keys()) == []

    # But the subtree that still exists should work fine
    jk09_sub = config['telescopes']['jk09']['components']
    assert jk09_sub.ccd.width == 512


@pytest.mark.asyncio
async def test_livedocument_partial_subtree_removal():
    """Test when only part of a subtree is removed."""
    config = LiveDocument({
        'telescopes': {
            'jk15': {
                'components': {
                    'ccd': {
                        'width': 1024,
                        'height': 1024
                    },
                    'mount': {
                        'type': 'equatorial'
                    },
                    'filter_wheel': {
                        'slots': 8
                    }
                }
            }
        }
    })

    # Get reference to components
    components = config.telescopes.jk15.components

    # Verify all components exist
    assert components.ccd.width == 1024
    assert components.mount.type == 'equatorial'
    assert components.filter_wheel.slots == 8

    # Update: remove only the mount, keep ccd and filter_wheel
    await config._update({
        'telescopes': {
            'jk15': {
                'components': {
                    'ccd': {
                        'width': 1024,
                        'height': 1024
                    },
                    'filter_wheel': {
                        'slots': 8
                    }
                }
            }
        }
    })

    # CCD and filter_wheel should still work
    assert components.ccd.width == 1024
    assert components.filter_wheel.slots == 8

    # Mount should raise error
    with pytest.raises(AttributeError):
        _ = components.mount

    with pytest.raises(KeyError):
        _ = components['mount']


@pytest.mark.asyncio
async def test_livedocument_deeply_removed_subtree_access():
    """Test accessing various levels of a removed deep hierarchy."""
    config = LiveDocument({
        'level1': {
            'level2': {
                'level3': {
                    'level4': {
                        'value': 'deep'
                    }
                }
            }
        }
    })

    # Get references at different levels
    level1 = config.level1
    level2 = config.level1.level2
    level3 = config.level1.level2.level3
    level4 = config.level1.level2.level3.level4

    # All should work
    assert level1.level2.level3.level4.value == 'deep'
    assert level2.level3.level4.value == 'deep'
    assert level3.level4.value == 'deep'
    assert level4.value == 'deep'

    # Remove everything by updating to empty
    await config._update({})

    # All old references should now be empty or raise errors
    assert len(level1) == 0
    assert len(level2) == 0
    assert len(level3) == 0
    assert len(level4) == 0

    with pytest.raises(AttributeError):
        _ = level1.level2

    with pytest.raises(AttributeError):
        _ = level2.level3

    with pytest.raises(AttributeError):
        _ = level3.level4

    with pytest.raises(AttributeError):
        _ = level4.value


@pytest.mark.asyncio
async def test_livedocument_reappearing_subtree():
    """Test when a subtree is removed and then re-added."""
    config = LiveDocument({
        'database': {
            'host': 'localhost',
            'port': 5432
        }
    })

    # Get reference to database
    db = config.database
    assert db.host == 'localhost'
    assert db.port == 5432

    # Remove database entirely
    await config._update({})

    # Old reference should be empty
    assert len(db) == 0
    with pytest.raises(AttributeError):
        _ = db.host

    # Re-add database with different values
    await config._update({
        'database': {
            'host': 'remotehost',
            'port': 3306,
            'user': 'admin'
        }
    })

    # The old reference 'db' should now have the NEW values!
    # Because the update propagates to the same cached object
    assert db.host == 'remotehost'
    assert db.port == 3306
    assert db.user == 'admin'

    # And getting a fresh reference should be the same object
    db2 = config.database
    assert db is db2
