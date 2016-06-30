# Adapted from
# https://github.com/trbs/fallocate/issues/4

import ctypes
import ctypes.util
import logging
import os

_log = logging.getLogger(__name__)

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def _fake_fallocate(fd, mode, offset, len_):
    if len_ <= 0:
        raise IOError('fallocate: length must be positive')
    if mode & FALLOC_FL_PUNCH_HOLE:
        old = fd.tell()
        fd.seek(offset)
        fd.write(b'\x00' * len_)
        fd.seek(old)
    else:
        raise NotImplementedError(
            'fake fallocate() supports only hole punching')


def _make_fallocate():
    libc_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(libc_name, use_errno=True)
    _fallocate = libc.fallocate
    c_off_t = ctypes.c_size_t
    _fallocate.restype = ctypes.c_int
    _fallocate.argtypes = [ctypes.c_int, ctypes.c_int, c_off_t, c_off_t]

    def fallocate(fd, mode, offset, len_):
        if len_ <= 0:
            raise IOError('fallocate: length must be positive')
        res = _fallocate(fd.fileno(), mode, offset, len_)
        if res != 0:
            errno = ctypes.get_errno()
            raise OSError(errno, 'fallocate: ' + os.strerror(errno))
    return fallocate

try:
    fallocate = _make_fallocate()
except AttributeError:  # pragma: no cover
    _log.debug('Falling back to non-hole-punching `fallocate`.')
    fallocate = _fake_fallocate


def punch_hole(f, offset, len_):
    """Ensure that the specified byte range is zeroed.

    Depending on the availability of fallocate(), this is either
    delegated to the kernel or done manualy.
    """
    params = (f, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, len_)
    try:
        fallocate(*params)
    except OSError:
        _fake_fallocate(*params)
