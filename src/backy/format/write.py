
class Writer(object):
    existed = True # wether the outfile already existed or not.
    old_mtime = 0

    def __init__(self, filename, chunksize=backy.CHUNKSIZE):
        if not os.path.exists(filename):
            # create the file.
            file(filename, "wb")
            self.existed = False
        else:
            self.old_mtime = os.path.getmtime(filename)
            # update mtime
            os.utime(filename, None)

        self.f = file(filename, "rwb+")

        self.chunksize = chunksize
        posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)
        self.max_chunk_id = 0

    def setChunk(self, chunk_id, data):
        self.max_chunk_id = max(chunk_id, self.max_chunk_id)
        here = self.f.tell()
        there = chunk_id * self.chunksize
        if here != there:
            self.f.seek(there)
        #print "Writing to %d (%.2fMB)" % (here, len(data))
        self.f.write(data)
        # clear buffers for that action
        posix_fadvise(self.f.fileno(), 0, self.f.tell(), POSIX_FADV_DONTNEED)
        Stats().t(len(data))

    def getChunk(self, chunk_id):
        self.max_chunk_id = max(chunk_id, self.max_chunk_id)
        here = self.f.tell()
        there = chunk_id * self.chunksize
        if here != there:
            self.f.seek(there)
        #print "Reading from %d" % (here)
        data = self.f.read(self.chunksize)
        # clear buffers for that action
        posix_fadvise(self.f.fileno(), 0, self.f.tell(), POSIX_FADV_DONTNEED)
        Stats().t(len(data))
        return data

    def truncate(self, size=None):
        if size is None:
            size = self.max_chunk_id * self.chunksize + self.chunksize
        self.f.truncate(size) # in case we shrinked

    def size(self):
        # returns the size in bytes.
        here = self.f.tell()
        self.f.seek(0, 2)
        size = self.f.tell()
        self.f.seek(here)
        return size

    def close(self):
        self.f.close()

