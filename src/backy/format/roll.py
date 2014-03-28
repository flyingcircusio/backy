import backy
import hashlib


class Roll(object):

    roll = None

    def __init__(self):
        self.roll = []

    def _hash(self, data):
        return hashlib.md5(data).hexdigest()

    def setChunk(self, chunk_id, data):
        """ Calculates the sum of data and stores it for chunk_id.
        """
        hash = self._hash(data)
        l = len(self.roll)
        if l == chunk_id:
            self.roll.append(hash)
        elif len(self.roll) < chunk_id:
            # fill self.roll if it's too small
            self.roll += ["\0"*16 for x in xrange(chunk_id-l)]
            self.roll.append(hash)
        else:
            old = self.roll[chunk_id]
            self.roll[chunk_id] = hash
            if old == hash:
                return True
        return False

    def getChunk(self, chunk_id):
        try:
            return self.roll[chunk_id]
        except IndexError:
            return None

    def chunkMatches(self, chunk_id, data):
        """ Checks if the data is the same for the local chunk_id and data.
            Returns True if the data hash matches, False if not.
        """
        left = self.getChunk(chunk_id)
        if not left:
            return False
        right = self._hash(data)
        return left == right

    def appendHash(self, hash):
        """ this is used when reading a roll.
        """
        self.roll.append(hash)


class Rollfile(Roll):

    chunksize = backy.CHUNKSIZE
    original_filename = ""

    def __init__(self, filename, original_filename=None,
                 chunksize=backy.CHUNKSIZE):
        super(Rollfile, self).__init__()
        self.filename = filename
        try:
            self.roll = map(str.rstrip, file(filename, "rb").readlines())
            # XXX Stats().t(len(self.roll)*32)
        except IOError:
            # file doesn't exist. Create now to see if we are allowed to.
            try:
                file(filename, "wb")
            except IOError:
                # XXX show original error
                raise IOError(
                    "Couldn't create Rollfile. Please check permissions: %s"
                    % self.filename)
            self.original_filename = original_filename
            self.chunksize = chunksize
        else:
            if len(self.roll) < 2:
                raise IOError(
                    "Rollfile is invalid. Please remove: %s" % self.filename)
            self.original_filename = self.roll.pop(0)
            self.chunksize = self.roll.pop(0)

    def dump(self):
        yield "%s\n" % self.original_filename
        yield "%s\n" % self.chunksize
        for r in self.roll:
            yield "%s\n" % r

    def write(self):
        data = self.dump()
        f = file(self.filename, "w")
        f.writelines(data)
        # XXX Stats().t(f.tell())
        f.close()
