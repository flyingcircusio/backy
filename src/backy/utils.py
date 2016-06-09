from .ext_deps import CP
from .fallocate import punch_hole
import contextlib
import datetime
import hashlib
import logging
import mmap
import os
import os.path
import pytz
import random
import subprocess
import sys
import tempfile


logger = logging.getLogger(__name__)


class SafeFile(object):
    """A context manager for handling files in our
    scenarios more safely:

    - manage use of temporary files and os.rename for committing
    - ensure files are flushed to disk when closing
    - ensure files are write-protected (and maybe temporarily not)

    Use this as a context manager and then call the various open_*
    methods or use_write_protection() to enable the safety belts.

    use_write_protection() has to be called before opening.
    """

    protected_mode = 0o440

    def __init__(self, filename, encoding=None):
        self.filename = filename
        self.encoding = encoding
        self.f = None

    def __enter__(self):
        assert self.f is None
        self.rename = False
        self.write_protected = False
        self.commit_callbacks = []
        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        # Error handling:
        # - if we're based on temporary files, we simply unlink
        #   and haven't changed the original
        # - if we're working with the original, we at least have
        #   synced by now and will restore write-protection
        if self.f is None:
            return
        self.f.flush()
        os.fsync(self.f)
        self.f.close()

        if self.use_write_protection:
            os.chmod(self.f.name, self.protected_mode)
            if os.path.exists(self.filename):
                os.chmod(self.filename, self.protected_mode)

        if self.rename:
            if exc_type is None:
                os.rename(self.f.name, self.filename)
            else:
                os.unlink(self.f.name)

        self.f = None

    # Activate the different safety features

    def open_new(self, mode):
        """Open this as a new (temporary) file that will be renamed on close.
        """
        assert not self.f
        self.f = tempfile.NamedTemporaryFile(
            mode,
            dir=os.path.dirname(self.filename),
            delete=False)
        self.rename = True

    def open_copy(self, mode):
        """Open an existing file, make a copy first, rename on close."""
        assert not self.f

        if os.path.exists(self.filename):
            self.open_new('wb')
            with open(self.filename, 'rb', buffering=0) as source:
                safe_copy(source, self.f)
            os.fsync(self.f)
            self.f.close()

        self.f = open(self.f.name, mode, buffering=0)

    def open_inplace(self, mode):
        """Open as an existing file, in-place."""
        assert not self.f
        if self.use_write_protection and os.path.exists(self.filename):
            # This is kinda dangerous, but this is a tool that only protects
            # you so far and doesn't try to get in your way if you really need
            # to do this.
            os.chmod(self.filename, 0o640)
        self.f = open(self.filename, mode, buffering=0)

    def use_write_protection(self):
        """Enable write-protection handling.

        Ensures you can do your work (non-write-protected) and ensures to
        (re-)enable write protection on close.

        """
        assert not self.f
        self.write_protected = True

    # File API shim

    @property
    def name(self):
        return self.f.name

    def read(self, *args, **kw):
        data = self.f.read(*args, **kw)
        if self.encoding:
            data = data.decode(self.encoding)
        return data

    def write(self, data):
        if self.encoding:
            data = data.encode(self.encoding)
        self.f.write(data)

    def seek(self, offset, whence=0):
        return self.f.seek(offset, whence)

    def tell(self):
        return self.f.tell()

    def truncate(self, size=None):
        return self.f.truncate(size)

    def fileno(self):
        return self.f.fileno()


Bytes = 1
kiB = Bytes * 1024
MiB = kiB * 1024
GiB = MiB * 1024
TiB = GiB * 1024
# Conversion, Suffix, Format,  Plurals
BYTE_UNITS = [
    (Bytes, 'Byte', '%d', True),
    (kiB, 'kiB', '%0.2f', False),
    (MiB, 'MiB', '%0.2f', False),
    (GiB, 'GiB', '%0.2f', False),
    (TiB, 'TiB', '%0.2f', False)
]


def safe_symlink(realfile, link):
    """Set a symlink unconditionally.

    If the link used to exit before, replace it. Make the symlink
    relative if possible.
    """
    try:
        os.unlink(link)
    except OSError:
        pass
    os.symlink(os.path.relpath(realfile, os.path.dirname(link)), link)


def format_bytes_flexible(number):
    for factor, label, format, plurals in BYTE_UNITS:
        if (number / factor) < 1024:
            break
    if plurals and number != 1:
        label += 's'
    return '%s %s' % (format % (number / factor), label)


# 16 kiB blocks are a good compromise between sparsiness and fragmentation.
PUNCH_SIZE = 16 * kiB
CHUNK_SIZE = 4 * MiB


if hasattr(os, 'posix_fadvise'):
    posix_fadvise = os.posix_fadvise
else:  # pragma: no cover
    logger.warn('Running without `posix_fadvise`.')
    os.POSIX_FADV_RANDOM = None
    os.POSIX_FADV_SEQUENTIAL = None
    os.POSIX_FADV_WILLNEED = None
    os.POSIX_FADV_DONTNEED = None

    def posix_fadvise(*args, **kw):
        return


if sys.platform == 'darwin':  # pragma: no cover
    @contextlib.contextmanager
    def zeroes(size):
        yield '\00' * size
else:
    @contextlib.contextmanager
    def zeroes(size):
        with open('/dev/zero', 'rb', buffering=0) as f:
            z = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            yield z[0:size]
            z.close()


def safe_copy(source, target):
    posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
    with zeroes(PUNCH_SIZE) as z:
        while True:
            chunk = source.read(CHUNK_SIZE)
            if not chunk:
                break
            # Search for zeroes that we can make sparse.
            pat_offset = 0
            while True:
                if chunk[pat_offset:pat_offset + PUNCH_SIZE] == z:
                    punch_hole(target, target.tell(), PUNCH_SIZE)
                    target.seek(PUNCH_SIZE, 1)
                else:
                    target.write(chunk[pat_offset:pat_offset + PUNCH_SIZE])
                pat_offset += PUNCH_SIZE
                if pat_offset > len(chunk):
                    break
    try:
        target.truncate()
    except OSError:
        # truncate may not be supported, i.e. on special files
        pass
    size = target.tell()
    os.fsync(target)
    return size


def cp_reflink(source, target):
    """Makes as COW copy of `source` if COW is supported."""
    # We can't tell if reflink is really supported. It depends on the
    # filesystem.
    try:
        with open('/dev/null', 'wb') as devnull:
            subprocess.check_output([CP, '--reflink=always', source, target],
                                    stderr=devnull)
    except subprocess.CalledProcessError:
        logger.warn('Performing non-COW copy: %s -> %s', source, target)
        if os.path.exists(target):
            os.unlink(target)
        subprocess.check_call([CP, source, target])


def files_are_equal(a, b):
    posix_fadvise(a.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
    posix_fadvise(b.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
    position = 0
    errors = 0
    while True:
        chunk_a = a.read(CHUNK_SIZE)
        chunk_b = b.read(CHUNK_SIZE)
        if chunk_a != chunk_b:
            logger.error(
                "Chunk A ({}, {}) != Chunk B ({}, {}) "
                "at position {}".format(
                    repr(chunk_a), hashlib.md5(chunk_a).hexdigest(),
                    repr(chunk_b), hashlib.md5(chunk_b).hexdigest(),
                    position))
            errors += 1
        if not chunk_a:
            break
        position += CHUNK_SIZE
    return not errors


def files_are_roughly_equal(a, b, samplesize=0.01, blocksize=16 * kiB,
                            timeout=5 * 60):
    # We limit this to a 5 minute operation to avoid clogging the storage
    # infinitely.
    started = now()
    max_duration = datetime.timedelta(timeout)

    a.seek(0, 2)
    size = a.tell()
    blocks = size // blocksize
    sample = range(0, max(blocks, 1))
    sample = random.sample(sample, max(int(samplesize * blocks), 1))
    sample.sort()

    # turn off readahead
    for fdesc in a.fileno(), b.fileno():
        posix_fadvise(fdesc, 0, 0, os.POSIX_FADV_RANDOM)

    for block in sample:
        if now() - started > max_duration:
            return True

        # Pre-load full blocks
        posix_fadvise(a.fileno(), block * blocksize, blocksize,
                      os.POSIX_FADV_WILLNEED)
        posix_fadvise(b.fileno(), block * blocksize, blocksize,
                      os.POSIX_FADV_WILLNEED)
        a.seek(block * blocksize)
        b.seek(block * blocksize)
        chunk_a = a.read(blocksize)
        chunk_b = b.read(blocksize)
        if chunk_a != chunk_b:
            logger.error(
                "Chunk A (md5: {}) != Chunk B (md5: {}) at position {}".
                format(hashlib.md5(chunk_a).hexdigest(),
                       hashlib.md5(chunk_b).hexdigest(),
                       a.tell()))
            break
    else:
        return True
    return False


def now():
    """A monkey-patchable version of 'datetime.datetime.now()' to
    support unit testing.

    Also, ensure that we'll always use timezone-aware objects.
    """
    return datetime.datetime.now(pytz.UTC)


def min_date():
    return pytz.UTC.localize(datetime.datetime.min)


def format_timestamp(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
