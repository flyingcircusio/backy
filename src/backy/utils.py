import os.path
import os
import tempfile


class SafeWritableFile():

    def __init__(self, filename, rename=True):
        self.filename = filename
        self.rename = rename

    def __enter__(self):
        if self.rename:
            self.f = tempfile.NamedTemporaryFile(
                'wb',
                dir=os.path.dirname(self.filename),
                delete=False)
        else:
            self.f = open(self.filename, 'wb')
        return self.f

    def __exit__(self, exc_type, exc_info, exc_tb):
        self.f.flush()
        os.fsync(self.f)
        tempname = self.f.name
        if self.rename:
            os.rename(tempname, self.filename)
        self.f.close()
