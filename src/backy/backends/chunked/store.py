import glob
import hashlib
import os.path


# A chunkstore, is responsible for all revisions for a single backup, for now.
# We can start having statistics later how much reuse between images is
# actually there and maybe extend this to multiple images. This then requires a
# lot more coordination about the users of the pool.


class Store(object):

    def __init__(self, path='/tmp/chunks'):
        self.path = path
        self.users = []
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def validate(self):
        for file in glob.glob(self.path + '/*.chunk'):
            file_hash = os.path.splitext(os.path.split(file)[1])[0]
            data = open(file, 'rb').read()
            hash = hashlib.new('sha256', data).hexdigest()
            assert file_hash == hash, "Content mismatch for {}".format(hash)

    def expunge(self):
        # Need to hold a global lock. May want to limit operations holding
        # the lock to a certain number of operations and then release the lock
        # so that others can write again. After that we would have to get
        # the lock again and then reload the list of our users.
        used_hashes = set()
        for user in self.users:
            used_hashes.update(user._mapping.values())

        for file in glob.glob(self.path + '/*.chunk'):
            file_hash = os.path.splitext(os.path.split(file)[1])[0]
            if file_hash not in used_hashes:
                os.unlink(file)

    def chunk_path(self, hash):
        return os.path.join(self.path, hash + '.chunk')
