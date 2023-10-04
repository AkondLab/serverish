import pytest
from serverish.base.fifoset import FifoSet


def test_fifoset_add_contian():
    """Test FifoSet add and element contains"""
    s = FifoSet(10)
    s.add(1)
    s.add(2)
    s.add(3)
    assert 1 in s
    assert 2 in s
    assert 3 in s
    assert 4 not in s


def test_fifoset_overflow():
    """Test FifoSet when capacity is reached"""
    s = FifoSet(3)
    s.add(1)
    s.add(2)
    s.add(3)
    s.add(4)
    assert 1 not in s
    assert 2 in s
    assert 3 in s
    assert 4 in s
