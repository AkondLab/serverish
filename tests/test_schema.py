from serverish.messenger import Messenger
from serverish.messenger.msgvalidator import DataValidator, MsgValidator


def test_dv_default():
    dv = DataValidator()
    dv.validate({'a': 1, 'b': 2})

def test_msgv_default():
    msgv = MsgValidator()
    msg = Messenger.create_msg(data={'a': 1, 'b': 2})
    msgv.validate(msg)


