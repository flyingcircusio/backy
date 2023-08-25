import asyncio
import contextlib
import datetime
import hashlib
import mmap
import os
import os.path
import random
import subprocess
import sys
import tempfile
import time
from typing import IO, Callable
from zoneinfo import ZoneInfo

import humanize
import structlog
import tzlocal

from .ext_deps import CP
from .fallocate import punch_hole

log = structlog.stdlib.get_logger(subsystem="utils")

log_data: str  # for pytest

Bytes = 1
kiB = Bytes * 1024
MiB = kiB * 1024
GiB = MiB * 1024
TiB = GiB * 1024

# 64 kiB blocks are a good compromise between sparsiness and fragmentation.
PUNCH_SIZE = 4 * MiB
CHUNK_SIZE = 4 * MiB

END = object()


def report_status(f):
    def wrapped(*args, **kw):
        generator = iter(f(*args, **kw))
        steps = next(generator)
        step = 0
        start = time.time()
        while True:
            status = next(generator)
            if status is END:
                break
            step += 1
            now = time.time()
            time_elapsed = now - start
            per_chunk = time_elapsed / step
            remaining = steps - step
            time_remaining = remaining * per_chunk
            eta = datetime.datetime.now() + datetime.timedelta(
                seconds=int(time_remaining)
            )
            if step == 5 or not step % 100:  # pragma: nocover
                log.info(
                    "progress-report",
                    step=step,
                    steps=steps,
                    percent=round(step / steps * 100, 2),
                    elapsed=round(time_elapsed),
                    remaining=humanize.naturaldelta(time_remaining),
                    eta=str(eta),
                )

        result = next(generator)
        return result

    return wrapped


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

    def __init__(self, filename, encoding=None, sync=True):
        self.filename = filename
        self.encoding = encoding
        self.sync = sync
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
        if self.sync:
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
        """Open this as a new (temporary) file that will be renamed on close."""
        assert not self.f
        self.f = tempfile.NamedTemporaryFile(
            mode, dir=os.path.dirname(self.filename), delete=False
        )
        self.rename = True

    def open_copy(self, mode):
        """Open an existing file, make a copy first, rename on close."""
        assert not self.f

        if os.path.exists(self.filename):
            self.open_new("wb")
            cp_reflink(self.filename, self.f.name)
            self.f.close()

        self.f = open(self.f.name, mode)

    def open_inplace(self, mode):
        """Open as an existing file, in-place."""
        assert not self.f
        if self.use_write_protection and os.path.exists(self.filename):
            # This is kinda dangerous, but this is a tool that only protects
            # you so far and doesn't try to get in your way if you really need
            # to do this.
            os.chmod(self.filename, 0o640)
        self.f = open(self.filename, mode)

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


if hasattr(os, "posix_fadvise"):
    posix_fadvise = os.posix_fadvise
else:  # pragma: no cover
    os.POSIX_FADV_RANDOM = None  # type: ignore
    os.POSIX_FADV_SEQUENTIAL = None  # type: ignore
    os.POSIX_FADV_WILLNEED = None  # type: ignore
    os.POSIX_FADV_DONTNEED = None  # type: ignore

    def posix_fadvise(*args, **kw):
        log.debug("posix_fadivse-unavailable")
        return


if sys.platform == "darwin":  # pragma: no cover

    @contextlib.contextmanager
    def zeroes(size):
        yield "\00" * size

else:

    @contextlib.contextmanager
    def zeroes(size):
        with open("/dev/zero", "rb", buffering=0) as f:
            z = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            yield z[0:size]
            z.close()


@report_status
def copy_overwrite(source, target):
    """Efficiently overwrites `target` with a copy of `source`.

    Identical regions won't be touched so this is COW-friendly. Assumes
    that `target` is a shallow copy of a previous version of `source`.

    Assumes that `target` exists and is open in read-write mode.
    """
    punch_size = PUNCH_SIZE
    source.seek(0, 2)
    size = source.tell()
    if size > 500 * GiB:
        punch_size *= 10
    source.seek(0)
    yield size / punch_size
    try:
        posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
    except Exception:
        pass
    with zeroes(punch_size) as z:
        while True:
            startpos = source.tell()
            chunk = source.read(punch_size)
            if not chunk:
                break
            target.seek(startpos)
            compare = target.read(len(chunk))
            if not compare or chunk != compare:
                if chunk == z:
                    target.flush()
                    punch_hole(target, startpos, len(chunk))
                else:
                    target.seek(startpos)
                    target.write(chunk)
            yield

    size = source.tell()
    target.flush()
    try:
        target.truncate(size)
    except OSError:
        pass  # truncate may not be supported, i.e. on special files
    try:
        os.fsync(target)
    except OSError:
        pass  # fsync may not be supported, i.e. on special files
    yield END
    yield size


@report_status
def copy(source, target):
    """Efficiently overwrites `target` with a copy of `source`.

    Identical regions will be touched - so this is not CoW-friendly.

    Assumes that `target` exists and is open in read-write mode.
    """

    chunk_size = 4 * 1024 * 1024
    source.seek(0, 2)
    yield source.tell() / chunk_size
    source.seek(0)

    try:
        posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
    except Exception:
        pass
    while True:
        chunk = source.read(chunk_size)
        if not chunk:
            break
        target.write(chunk)
        yield

    size = source.tell()
    target.flush()
    try:
        target.truncate(size)
    except OSError:
        pass  # truncate may not be supported, i.e. on special files
    try:
        os.fsync(target)
    except OSError:
        pass  # fsync may not be supported, i.e. on special files
    yield END
    yield size


def cp_reflink(source, target):
    """Makes as COW copy of `source` if COW is supported."""
    # We can't tell if reflink is really supported. It depends on the
    # filesystem.
    try:
        subprocess.check_call(
            [CP, "--reflink=always", source, target], stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError:
        log.warning("cp_reflink-no-cow", source=source, target=target)
        if os.path.exists(target):
            os.unlink(target)
        subprocess.check_call([CP, source, target])


def files_are_equal(
    a: IO,
    b: IO,
    report: Callable[[bytes, bytes, int], None] = lambda a, b, c: None,
) -> bool:
    try:
        posix_fadvise(a.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)  # type: ignore
        posix_fadvise(b.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)  # type: ignore
    except Exception:
        pass
    position = 0
    errors = 0
    while True:
        chunk_a = a.read(CHUNK_SIZE)
        chunk_b = b.read(CHUNK_SIZE)
        if chunk_a != chunk_b:
            log.error(
                "files-not-equal",
                hash_a=hashlib.md5(chunk_a).hexdigest(),
                hash_b=hashlib.md5(chunk_b).hexdigest(),
                pos=position,
            )
            report(chunk_a, chunk_b, position)
            errors += 1
        if not chunk_a:
            break
        position += CHUNK_SIZE
    return not errors


def files_are_roughly_equal(
    a: IO,
    b: IO,
    samplesize=0.01,
    blocksize=CHUNK_SIZE,
    timeout=5 * 60,
    report: Callable[[bytes, bytes, int], None] = lambda a, b, c: None,
) -> bool:
    a.seek(0, os.SEEK_END)
    size = a.tell()
    blocks = size // blocksize
    blocklist = range(0, max(blocks, 1))
    sample = random.sample(blocklist, max(int(samplesize * blocks), 1))

    # turn off readahead
    try:
        for fdesc in a.fileno(), b.fileno():
            posix_fadvise(fdesc, 0, 0, os.POSIX_FADV_RANDOM)  # type: ignore
    except Exception:
        pass

    # We limit this to a 5 minute operation to avoid clogging the storage
    # infinitely.
    started = now()
    max_duration = datetime.timedelta(seconds=timeout)

    for block in sample:
        log.debug("files-roughly-equal-verifying", offset=block * blocksize)
        duration = now() - started
        if duration > max_duration:
            log.info("files-roughly-equal-stopped", duration=duration)
            return True

        a.seek(block * blocksize)
        b.seek(block * blocksize)
        chunk_a = a.read(blocksize)
        chunk_b = b.read(blocksize)
        if chunk_a != chunk_b:
            log.error(
                "files-not-roughly-equal",
                hash_a=hashlib.md5(chunk_a).hexdigest(),
                hash_b=hashlib.md5(chunk_b).hexdigest(),
                pos=block * blocksize,
            )
            report(chunk_a, chunk_b, block * blocksize)
            return False
    return True


def now():
    """A monkey-patchable version of 'datetime.datetime.now()' to
    support unit testing.

    Also, ensure that we'll always use timezone-aware objects.
    """
    return datetime.datetime.now(ZoneInfo("UTC"))


def min_date():
    return datetime.datetime.min.replace(tzinfo=ZoneInfo("UTC"))


def has_recent_changes(entry, reference_time):
    # This is not efficient on a first look as we may stat things twice, but it
    # makes the recursion easier to read and the VFS will be caching this
    # anyway.
    # However, I want to perform a breadth-first analysis as the theory is that
    # higher levels will propagate changed mtimes do to new/deleted files
    # instead of just modified files in our case and looking at stats when
    # traversing a directory level is faster than going depth first.
    st = entry.stat(follow_symlinks=False)
    if st.st_mtime >= reference_time:
        return True
    if not entry.is_dir(follow_symlinks=False):
        return False
    candidates = list(os.scandir(entry.path))
    # First pass: stat all direct entries
    for candidate in candidates:
        if candidate.stat(follow_symlinks=False).st_mtime >= reference_time:
            return True
    # Second pass: start traversing
    for candidate in os.scandir(entry.path):
        if has_recent_changes(candidate, reference_time):
            return True
    return False


async def time_or_event(deadline, event):
    remaining_time = (deadline - now()).total_seconds()
    return await next(
        asyncio.as_completed([asyncio.sleep(remaining_time), event.wait()])
    )


def format_datetime_local(dt):
    tz = tzlocal.get_localzone()
    if dt is None:
        return "-", tz
    return (
        dt.astimezone(tz).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
        tz,
    )
