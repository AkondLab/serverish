from serverish.messenger import Messenger, get_publisher, get_reader


def test_messenger_get_xxx():
    # await ensure_stram_for_tests("srvh-test", "test.messenger.test_messenger_pub_simple")
    p = Messenger().get_publisher('test.messenger.test_messenger_get_xxx')
    r = Messenger().get_reader('test.messenger.test_messenger_get_xxx')

def test_get_xxx():
    # await ensure_stram_for_tests("srvh-test", "test.messenger.test_messenger_pub_simple")
    p = get_publisher('test.messenger.test_get_xxx')
    r = get_reader('test.messenger.test_get_xxx')


