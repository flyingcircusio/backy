import glob
import lzo
import os.path
import time
from . import chunk

# A chunkstore, is responsible for all revisions for a single backup, for now.
# We can start having statistics later how much reuse between images is
# actually there and maybe extend this to multiple images. This then requires a
# lot more coordination about the users of the pool.


def rreplace(str, old, new):
    return new.join(str.rsplit(old, 1))


class Store(object):

    def __init__(self, path='/tmp/chunks'):
        self.path = path
        self.users = []
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def validate_chunks(self):
        errors = 0
        chunks = list(self.ls())
        progress = 0
        start = time.time()
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
                yield ("Content mismatch. Expected {} got {}".format(
                    file_hash, hash), errors)
            progress += 1
            now = time.time()
            time_elapsed = now - start
            per_chunk = time_elapsed / progress
            remaining = len(chunks) - progress
            time_remaining = remaining * per_chunk
            if progress == 5 or not progress % 100:  # pragma: nocover
                yield ("Progress: {} of {} ({:.2f}%) "
                       "({:.0f}s elapsed, {:.0f}s remaining)".format(
                           progress, len(chunks), progress / len(chunks) * 100,
                           time_elapsed, time_remaining),
                       errors)
        return errors

    def ls(self):
        pattern = os.path.join(self.path, '*/*/*.chunk.lzo')
        for file in glob.glob(pattern):
            hash = rreplace(os.path.split(file)[1], '.chunk.lzo', '')
            yield file, hash, lambda f: lzo.decompress(open(f, 'rb').read())

    def purge(self):
        # This assumes exclusive lock on the store. Thisi s guaranteed by
        # backy's main locking.
        used_hashes = set()
        for user in self.users:
            used_hashes.update(user._mapping.values())

        unlinked = 0
        for file, file_hash, _ in self.ls():
            if file_hash not in used_hashes:
                os.unlink(file)
                unlinked += 1
            try:
                os.rmdir(os.path.dirname(file))
            except OSError:
                # Dir is not empty. Ignore.
                pass
        print("Purged: {} chunks".format(unlinked))

    def chunk_path(self, hash):
        dir1 = hash[:2]
        dir2 = hash[2:4]
        extension = '.chunk.lzo'
        return os.path.join(self.path, dir1, dir2, hash + extension)
