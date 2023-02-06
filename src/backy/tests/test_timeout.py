import pytest

from backy.timeout import TimeOut, TimeOutError


def test_timeout(capsys):
    timeout = TimeOut(0.05, 0.01)
    while timeout.tick():
        print("tick")
    assert timeout.timed_out
    out, err = capsys.readouterr()
    assert "tick\ntick\ntick" in out


def test_raise_on_timeout(capsys):
    timeout = TimeOut(0.05, 0.01, raise_on_timeout=True)
    with pytest.raises(TimeOutError):
        while True:
            timeout.tick()
            print("tick")
    out, err = capsys.readouterr()
    assert "tick\ntick\ntick" in out
