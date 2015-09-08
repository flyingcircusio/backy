# Taken from
# https://github.com/trbs/fallocate/issues/4 and adapted slightly to Python 3.4

import ctypes
import ctypes.util
import logging

logger = logging.getLogger(__name__)


def make_fallocate():
    c_off_t = ctypes.c_int64
    _fallocate = libc.fallocate
    _fallocate.restype = ctypes.c_int
    _fallocate.argtypes = [ctypes.c_int, ctypes.c_int, c_off_t, c_off_t]

    def fallocate(fd, mode, offset, len_):
        res = _fallocate(fd.fileno(), mode, offset, len_)
        if res != 0:
            raise IOError(res, 'fallocate')

    return fallocate

try:
    libc_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(libc_name)

    _fallocate = libc.fallocate
except AttributeError:
    logger.debug('Falling back to non-hole-punching `fallocate`.')

    def fallocate(fd, mode, offset, len_):
        old = fd.tell()
        fd.seek(offset)
        fd.write(b'\x00' * len_)
        fd.seek(old)
else:
    fallocate = make_fallocate()

del make_fallocate

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def punch_hole(f, from_, to):
    fallocate(f, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, from_, to)
