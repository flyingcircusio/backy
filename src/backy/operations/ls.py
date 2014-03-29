import glob
import os.path
import datetime


def ls(target):
    infile = target
    prefix = "%s.diff" % infile

    diffs = sorted(glob.glob("%s.*" % prefix), reverse=True)
    diffs = [
        d for d in diffs
        if not d.endswith(".index") and not d.endswith(".md5")]

    level = 0
    diffs.insert(0, infile)
    sumsize = 0
    for diff in diffs:
        mtime = os.path.getmtime(diff)
        size = os.path.getsize(diff)
        sumsize += size
        datacreated = (datetime.datetime.fromtimestamp(mtime).
                       strftime("%Y-%m-%d %H:%M:%S"))
        # index_filename = "%s.index" % diff
        print "Level %d from %s (%.1fGiB)" % (
            level, datacreated, float(size)/(1024**3))
        level += 1
    print
    print """\
Statistics: Backup contains %d levels with a total size of %.1fGiB""" % (
        level, float(sumsize)/(1024**3))
