import backy
import gzip
import os.path


class Patcher(object):
    """ Applies diffs to a chunk
    """

    matrix = None

    def __init__(self, diff_filenames):
        """ diff_filenames are the data filenames, i.e. NOT the index filename.
            Example: ['img.diff.1', 'img.diff.2']
            The diffs are applied i reverse numerical order (i.e. 3,2,1)
        """
        diff_filenames.sort(reverse=True)
        self.matrix = {}

        for diff_filename in diff_filenames:
            index_filename = "%s.index" % diff_filename
            if not os.path.exists(index_filename):
                # there was no difference in the backups. Just ignore.
                continue
            #print "Evaluating index %s." % index_filename
            index = file(index_filename, "r").readlines()
            # XXX Stats().t(len(index))
            index.pop(0)  # finished string
            chunksize = int(index.pop(0))
            # for now we do only support exactly one chunksize.
            if chunksize != backy.CHUNKSIZE:
                raise Exception("Don't mess with the chunksize.")
            for chunk in index:
                chunk = chunk.rstrip()
                chunk_id, offset, length = map(int, chunk.split(","))
                self.matrix[chunk_id] = {
                    'filename': diff_filename,
                    'offset': offset,
                    'length': length}

    def getChunk(self, chunk_id):
        """ Returns the patched chunk or None, if there's no need to patch.
        """
        diff = self.matrix.get(chunk_id)
        if not diff:
            return None
        f = file(diff['filename'], "rb")
        f.seek(diff['offset'])
        z = f.read(diff['length'])
        # Stats().t(len(z))
        data = gzip.zlib.decompress(z)
        f.close()
        return data
