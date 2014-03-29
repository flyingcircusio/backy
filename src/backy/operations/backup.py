from backy.format import Reader, Writer, Rollfile, Differ
import os.path
import hashlib
import shutil


def backup(source, target):
    infile = source
    outfile = target
    checkfile = "%s.md5" % outfile

    reader = Reader(infile)
    writer = Writer(outfile)
    roller = Rollfile(outfile)

    if writer.existed:
        differ = Differ(outfile, old_mtime=writer.old_mtime)

        if writer.size() != reader.size():
            # We have a problem here, because the rollfile does not reflect
            # the new size. For now, we raise here.
            raise Exception(
                "Source and destination sizes differ (%s:%d and %s:%d, "
                "respectively). Perhaps stale entries in /dev/mapper are "
                " causing an unlinked snapshot." % (
                    infile, reader.size(), outfile, writer.size()))

        if os.path.exists(checkfile):
            diff_checkfilename = "%s.md5" % differ.filename
            shutil.copy(checkfile, diff_checkfilename)

    i = 0
    checksum = hashlib.md5()
    while True:
        data = reader.getChunk(i)
        checksum.update(data)

        if not data:
            break
        if not roller.chunkMatches(i, data):
            # only diff if the writerfile existed
            if writer.existed:
                old_data = writer.getChunk(i)
                differ.setChunk(i, old_data)

            writer.setChunk(i, data)
            roller.setChunk(i, data)
            # write roller every 16 chunks
            if i % 16 == 0:
                roller.write()
            # XXX print "Writing chunk %06d" % i
        else:
            # XXX print "Chunk matches: %06d" % i
            pass
        i += 1

    # this checksum is for the currently read image
    file(checkfile, "w").write(checksum.hexdigest())

    if writer.existed:
        differ.close()
    roller.write()
    reader.close()
    writer.close()
