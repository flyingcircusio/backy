import glob
import os


def clean(target, revision, diffprefix=None, force=False):
    infile = target
    level = revision
    prefix = diffprefix

    if prefix is None:
        prefix = "%s.diff" % infile

    num_keep = int(level)
    if num_keep < 1 and not force:
        raise ValueError("You must keep at least one diff.")

    diffs = sorted(glob.glob("%s.*" % prefix), reverse=True)
    diffs = [
        d for d in diffs
        if not d.endswith(".index") and not d.endswith(".md5")]

    delete = diffs[num_keep:]
    for d in delete:
        files = sorted(glob.glob("%s*" % d))
        for f in files:
            print "Deleting: %s" % f
            os.unlink(f)
