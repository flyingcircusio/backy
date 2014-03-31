import backy.backup


def test_format_timestamp():
    assert '1970-01-01 01:00:00' == backy.backup.format_timestamp(0)
