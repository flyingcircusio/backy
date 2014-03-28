from backy.format import Patcher, Reader, Writer
import hashlib
import glob
import os.path


def _restore(infile, outfile, difffiles, force):

    patcher = Patcher(difffiles)
    reader = Reader(infile)
    writer = Writer(outfile)
    if writer.existed and not force:
        raise IOError("Outfile existed. Will not overwrite.")

    # XXX refactor patcher
    i = 0
    checksum = hashlib.md5()
    while True:
        data = patcher.getChunk(i)
        if not data:
            data = reader.getChunk(i)
        if not data:
            break
        checksum.update(data)

        writer.setChunk(i, data)
        # print "Writing chunk %06d" % i
        i += 1

    reader.close()
    writer.close()
    return checksum.hexdigest()


def restore(source, target, revision, checkonly=False, diffprefix=None,
            force=None):
    infile = source
    outfile = target
    level = revision
    prefix = diffprefix

    if prefix is None:
        prefix = "%s.diff" % infile

    diffs = sorted(glob.glob("%s.*" % prefix), reverse=True)
    diffs = [
        d for d in diffs
        if not d.endswith(".index") and not d.endswith(".md5")]
    if level > len(diffs):
        raise ValueError(
            "We don't have a backup of level %d. Maximum level is %d." %
            (level, len(diffs)))
    to_restore = diffs[:level]

    # find out target checksum from oldest diff or - if level=0 - from image
    target_checksum_file = "%s.md5" % infile
    target_checksum = ""
    if os.path.exists(target_checksum_file):
        target_checksum = file(target_checksum_file, "r").read()
    if level > 0:
        print "Using diffs: %s" % to_restore
        oldest_diff = to_restore[-1]
        target_checksum_file = "%s.md5" % oldest_diff
        target_checksum = file(target_checksum_file, "r").read()

    checksum = _restore(infile, outfile, to_restore, force)

    if target_checksum and target_checksum != checksum:
        raise Exception(
            "ERROR: Checksums don't match. Restore image is invalid.")
    elif target_checksum:
        print "Restore checksum validated. Restore image is valid."
