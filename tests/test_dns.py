"""Tests for DNS resolution features provided by the Connection class."""
import asyncio

import aiodns
import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.connection import Connection


async def test_dns():
    resolver = aiodns.DNSResolver()
    ip = await Connection._check_host(resolver, 'google.com')
    assert ip is None
