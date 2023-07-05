import glob
import os.path

import lzo
from structlog.stdlib import BoundLogger

from backy.utils import END, report_status

from . import File, chunk

# A chunkstore, is responsible for all revisions for a single backup, for now.
# We can start having statistics later how much reuse between images is
# actually there and maybe extend this to multiple images. This then requires a
# lot more coordination about the users of the pool.


def rreplace(str, old, new):
    return new.join(str.rsplit(old, 1))


class Store(object):
    # Signal that we should always override chunks that we want to write.
    # This can be used in the face of suspected inconsistencies while still
    # wanting to perform new backups.
    force_writes = False

    path: str
    users: list[File]
    seen_forced: set[str]
    known: set[str]
    log: BoundLogger

    def __init__(self, path, log):
        self.path = path
        self.users = []
        self.seen_forced = set()
        self.known = set()
        self.log = log.bind(subsystem="chunked-store")
        for x in range(256):
            subdir = os.path.join(self.path, f"{x:02x}")
            if not os.path.exists(subdir):
                try:
                    os.makedirs(subdir)
                except FileExistsError:
                    pass
        if not os.path.exists(os.path.join(self.path, "store")):
            self.convert_to_v2()

        for c in glob.iglob(os.path.join(self.path, "*/*.chunk.lzo")):
            hash = os.path.basename(c).replace(".chunk.lzo", "")
            self.known.add(hash)
        self.log.debug("init", known_chunks=len(self.known))

    def convert_to_v2(self):
        self.log.info("to-v2")
        for path in glob.iglob(os.path.join(self.path, "*/*/*.chunk.lzo")):
            new = path.replace(self.path + "/", "", 1)
            dir, _, c = new.split("/")
            new = os.path.join(self.path, dir, c)
            os.rename(path, new)
        for path in glob.iglob(os.path.join(self.path, "*/??")):
            if os.path.isdir(path):
                os.rmdir(path)
        with open(os.path.join(self.path, "store"), "wb") as f:
            f.write(b"v2")
        self.log.info("to-v2-finished")

    @report_status
    def validate_chunks(self):
        errors = 0
        chunks = list(self.ls())
        yield len(chunks)
        for file, file_hash, read in chunks:
            try:
                data = read(file)
            except Exception:
                # Typical Exceptions would be IOError, TypeError (lzo)
                hash = None
            else:
                hash = chunk.hash(data)
            if file_hash != hash:
                errors += 1
                yield (
                    "Content mismatch. Expected {} got {}".format(
                        file_hash, hash
                    ),
                    errors,
                )
            yield
        yield END
        yield errors

    def ls(self):
        # XXX this is fucking expensive
        pattern = os.path.join(self.path, "*/*.chunk.lzo")
        for file in glob.iglob(pattern):
            hash = rreplace(os.path.split(file)[1], ".chunk.lzo", "")
            yield file, hash, lambda f: lzo.decompress(open(f, "rb").read())

    def purge(self):
        # This assumes exclusive lock on the store. This is guaranteed by
        # backy's main locking.
        to_delete = self.known.copy()
        for user in self.users:
            to_delete = to_delete - set(user._mapping.values())
        self.log.info("purge", purging=len(to_delete))
        for file_hash in sorted(to_delete):
            if os.path.exists(self.chunk_path(file_hash)):
                os.unlink(self.chunk_path(file_hash))
        self.known -= to_delete

    def chunk_path(self, hash):
        dir1 = hash[:2]
        extension = ".chunk.lzo"
        return os.path.join(self.path, dir1, hash + extension)
