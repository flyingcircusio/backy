from . import chunk
from backy.utils import report_status, END
import glob
import logging
import lzo
import os.path

logger = logging.getLogger(__name__)


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

    def __init__(self, path='/tmp/chunks'):
        self.path = path
        self.users = []
        self.seen_forced = set()
        for x in range(256):
            subdir = os.path.join(self.path, f'{x:02x}')
            if not os.path.exists(subdir):
                try:
                    os.makedirs(subdir)
                except FileExistsError:
                    pass
        if not os.path.exists(os.path.join(self.path, 'store')):
            self.convert_to_v2()

    def convert_to_v2(self):
        logger.info('Converting chunk store to v2')
        for path in glob.iglob(os.path.join(self.path, '*/*/*.chunk.lzo')):
            new = path.replace(self.path + '/', '', 1)
            dir, _, chunk = new.split('/')
            new = os.path.join(self.path, dir, chunk)
            os.rename(path, new)
        for path in glob.iglob(os.path.join(self.path, '*/??')):
            if os.path.isdir(path):
                os.rmdir(path)
        with open(os.path.join(self.path, 'store'), 'wb') as f:
            f.write(b'v2')
        logger.info('Finished conversion to v2')

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
                yield ("Content mismatch. Expected {} got {}".format(
                    file_hash, hash), errors)
            yield
        yield END
        yield errors

    def ls(self):
        # XXX this is fucking expensive
        pattern = os.path.join(self.path, '*/*.chunk.lzo')
        for file in glob.iglob(pattern):
            hash = rreplace(os.path.split(file)[1], '.chunk.lzo', '')
            yield file, hash, lambda f: lzo.decompress(open(f, 'rb').read())

    def purge(self):
        # This assumes exclusive lock on the store. This is guaranteed by
        # backy's main locking.
        used_hashes = set()
        for user in self.users:
            used_hashes.update(user._mapping.values())
        unlinked = 0
        for file, file_hash, _ in self.ls():
            if file_hash not in used_hashes:
                os.unlink(file)
                unlinked += 1
        print("Purged: {} chunks".format(unlinked))

    def chunk_path(self, hash):
        dir1 = hash[:2]
        extension = '.chunk.lzo'
        return os.path.join(self.path, dir1, hash + extension)
