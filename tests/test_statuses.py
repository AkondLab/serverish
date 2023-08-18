from serverish.status import Status, StatusEnum


def test_equal_Status():
    s1 = Status.new_ok(msg='s1')
    s2 = Status.new_ok(msg='s2')
    assert s1 == s2
    s3 = Status.new_fail(msg='s1')
    assert s1 != s3

def test_equal_Status_and_enum():
    s1 = Status.new_ok(msg='s1')
    s2 = StatusEnum.ok
    s3 = 'ok'
    assert s1 == s2
    assert s1 == s3
    assert s2 == s3
    assert s1 == StatusEnum.ok
    s4 = Status.new_fail(msg='s1')
    assert s4 != s2
    assert s4 != s3


