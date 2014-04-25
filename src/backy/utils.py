import os.path
import os
import tempfile


class SafeWritableFile():

    def __init__(self, filename, rename=True, write_protect=False,
                 encoding=None):
        self.filename = filename
        self.rename = rename
        self.write_protect = write_protect
        self.encoding = encoding

    def __enter__(self):
        if self.rename:
            self.f = tempfile.NamedTemporaryFile(
                'wb',
                encoding=self.encoding,
                dir=os.path.dirname(self.filename),
                delete=False)
        else:
            if self.write_protect and os.path.exists(self.filename):
                os.chmod(self.filename, 0o640)
            self.f = open(self.filename, 'wb', encoding=self.encoding)
        return self.f

    def __exit__(self, exc_type, exc_info, exc_tb):
        self.f.flush()
        os.fsync(self.f)
        tempname = self.f.name
        if self.rename:
            if self.write_protect:
                os.chmod(self.filename, 0o640)
            os.rename(tempname, self.filename)
        self.f.close()
        if self.write_protect:
            os.chmod(self.filename, 0o440)


# XXX duplicated from unit.py
Bytes = 1.0
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
