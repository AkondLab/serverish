import asyncio

import pytest
import socket


from serverish.connection import Connection
from serverish.connection_jets import ConnectionJetStream
from serverish.connection_nats import ConnectionNATS
from serverish.messenger import Messenger
from serverish.msgvalidator import DataValidator, MsgValidator


def test_dv_default():
    dv = DataValidator()
    dv.validate({'a': 1, 'b': 2})

def test_msgv_default():
    msgv = MsgValidator()
    msg = Messenger.create_msg(data={'a': 1, 'b': 2})
    msgv.validate(msg)


