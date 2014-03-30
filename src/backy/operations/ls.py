import datetime
import backy.backup


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

# XXX also show whether revisions are known to have bad blocks


def ls(target):
    backup = backy.backup.Backup(target)
    revisions = backup.revisions.values()
    revisions.sort(key=lambda r: r.timestamp)

    total_blocks = 0

    print "== Revisions"
    for r in revisions:
        print "{}\t{}\t{}".format(
            format_timestamp(r.timestamp),
            len(r.blocksums),
            r.uuid)
        total_blocks += len(r.blocksums)

    print
    print "== Summary"
    print "{} revisions with {} blocks (~{} blocks/revision)".format(
        len(revisions), total_blocks, total_blocks/len(revisions))
