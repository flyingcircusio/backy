import os
import os.path
import random
import tempfile


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
        self.f.flush()
        os.fsync(self.f)
        self.f.close()

        if self.use_write_protection:
            os.chmod(self.f.name, 0o440)
            if os.path.exists(self.filename):
                os.chmod(self.filename, 0o440)

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
            source = open(self.filename, 'rb')
            safe_copy(source, self.f)
            source.close()
            self.f.flush()
            os.fsync(self.f)
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

    def seek(self, *args, **kw):
        return self.f.seek(*args, **kw)

    def truncate(self, *args, **kw):
        return self.f.truncate(*args, **kw)


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


def format_bytes_flexible(number):
    for factor, label, format, plurals in BYTE_UNITS:
        if (number / factor) < 1024:
            break
    if plurals and number != 1:
        label += 's'
    return '%s %s' % (format % (number / factor), label)


def safe_copy(source, target):
    while True:
        chunk = source.read(4*1024**2)
        if not chunk:
            break
        target.write(chunk)
    target.truncate()
    size = target.tell()
    target.flush()
    os.fsync(target)
    return size


def files_are_equal(a, b):
    while True:
        chunk_a = a.read(4*MiB)
        chunk_b = b.read(4*MiB)
        if chunk_a != chunk_b:
            return False
        if not chunk_a:
            break
    return True


def files_are_roughly_equal(a, b, samplesize=0.05, blocksize=4*MiB):
    a.seek(0, 2)
    size = a.tell()
    blocks = size // blocksize
    sample = range(0, blocks)
    sample = random.sample(sample, int(samplesize * blocks))
    for block in sample:
        a.seek(block * blocksize)
        b.seek(block * blocksize)
        if not a.read(blocksize) == b.read(blocksize):
            break
    else:
        return True
    return False
