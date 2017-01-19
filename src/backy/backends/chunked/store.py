import glob
import hashlib
import os.path
import gzip

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

    def validate(self):
        for file, file_hash, open in self.ls():
            data = open(file, 'rb').read()
            hash = hashlib.new('sha256', data).hexdigest()
            assert file_hash == hash, "Content mismatch for {}".format(hash)

    def ls(self):
        for file in glob.glob(self.path + '/*.chunk'):
            hash = rreplace(os.path.split(file)[1], '.chunk', '')
            yield file, hash, open
        for file in glob.glob(self.path + '/*.chunk.gz'):
            hash = rreplace(os.path.split(file)[1], '.chunk.gz', '')
            yield file, hash, gzip.open

    def expunge(self):
        # Need to hold a global lock. May want to limit operations holding
        # the lock to a certain number of operations and then release the lock
        # so that others can write again. After that we would have to get
        # the lock again and then reload the list of our users.
        used_hashes = set()
        for user in self.users:
            used_hashes.update(user._mapping.values())

        unlinked = 0
        for file, file_hash, _ in self.ls():
            if file_hash not in used_hashes:
                os.unlink(file)
                unlinked += 1
        print("Expunged: {} chunks".format(unlinked))

    def chunk_path(self, hash, compressed=True):
        extension = '.chunk.gz' if compressed else '.chunk'
        return os.path.join(self.path, hash + extension)
